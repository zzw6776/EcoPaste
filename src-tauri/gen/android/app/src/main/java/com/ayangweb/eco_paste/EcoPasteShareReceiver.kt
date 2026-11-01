package com.ayangweb.eco_paste

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.provider.OpenableColumns
import android.text.Html
import android.util.Log
import android.webkit.MimeTypeMap
import android.widget.Toast
import androidx.core.content.IntentCompat
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import org.json.JSONArray
import org.json.JSONObject

/** Reads temporary Android content grants off the UI thread and hands owned copies to Rust. */
object EcoPasteShareReceiver {
    private val executor = Executors.newSingleThreadExecutor()
    private val runtimeReady = CompletableFuture<Unit>()

    fun onRuntimeReady() {
        runtimeReady.complete(Unit)
    }

    /** Queues each delivery once; opening the launcher or a backup via ACTION_VIEW is unrelated. */
    fun receive(context: Context, intent: Intent?) {
        if (intent?.action != Intent.ACTION_SEND && intent?.action != Intent.ACTION_SEND_MULTIPLE) {
            return
        }
        val application = context.applicationContext
        val delivery = Intent(intent)
        executor.execute { importDelivery(application, delivery) }
    }

    /** Keeps source URI permissions in use only while streaming into the app's private cache. */
    private fun importDelivery(context: Context, intent: Intent) {
        val stagingRoot = File(context.cacheDir, "ecopaste-share")
        var staging: File? = null
        val payload = JSONObject()
        var failed = 0
        try {
            val uris = linkedSetOf<Uri>()
            if (intent.action == Intent.ACTION_SEND_MULTIPLE) {
                IntentCompat.getParcelableArrayListExtra(intent, Intent.EXTRA_STREAM, Uri::class.java)
                    ?.let { uris.addAll(it) }
            } else {
                IntentCompat.getParcelableExtra(intent, Intent.EXTRA_STREAM, Uri::class.java)
                    ?.let { uris.add(it) }
            }
            val clip = intent.clipData
            val textShareHasPreview = intent.type?.startsWith("text/") == true &&
                (intent.hasExtra(Intent.EXTRA_TEXT) || intent.hasExtra(Intent.EXTRA_HTML_TEXT))
            if (uris.isEmpty() && clip != null && !textShareHasPreview) {
                for (index in 0 until clip.itemCount) {
                    clip.getItemAt(index).uri?.let { uris.add(it) }
                }
            }

            val texts = linkedSetOf<String>()
            if (intent.action == Intent.ACTION_SEND_MULTIPLE) {
                intent.getCharSequenceArrayListExtra(Intent.EXTRA_TEXT)?.forEach { text ->
                    if (text.isNotBlank()) texts.add(text.toString())
                }
            }
            intent.getCharSequenceExtra(Intent.EXTRA_TEXT)?.toString()
                ?.takeIf { it.isNotBlank() }?.let { texts.add(it) }
            if (texts.isEmpty() && clip != null) {
                for (index in 0 until clip.itemCount) {
                    clip.getItemAt(index).text?.toString()
                        ?.takeIf { it.isNotBlank() }?.let { texts.add(it) }
                }
            }
            val html = intent.getStringExtra(Intent.EXTRA_HTML_TEXT)
            if (texts.isEmpty() && !html.isNullOrBlank()) {
                texts.add(Html.fromHtml(html, Html.FROM_HTML_MODE_LEGACY).toString())
            }
            payload.put("texts", JSONArray(texts.toList()))
            payload.put("html", html)

            val files = JSONArray()
            if (uris.isNotEmpty()) {
                check(stagingRoot.isDirectory || stagingRoot.mkdirs())
                staging = File(stagingRoot, java.util.UUID.randomUUID().toString())
                check(staging.mkdirs())
            }
            uris.forEachIndexed { index, uri ->
                val target = File(checkNotNull(staging), index.toString())
                try {
                    // Only provider-granted content is accepted; never read arbitrary file:// paths.
                    require(uri.scheme == "content")
                    var name: String? = null
                    try {
                        context.contentResolver.query(
                            uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null,
                        )?.use { cursor ->
                            val column = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                            if (column >= 0 && cursor.moveToFirst()) name = cursor.getString(column)
                        }
                    } catch (error: Exception) {
                        Log.d("EcoPasteShare", "Shared attachment has no display name", error)
                    }
                    val mime = try {
                        context.contentResolver.getType(uri) ?: intent.type
                    } catch (error: Exception) {
                        Log.d("EcoPasteShare", "Shared attachment has no MIME metadata", error)
                        intent.type
                    }
                    val extension = mime?.let { MimeTypeMap.getSingleton().getExtensionFromMimeType(it) }
                    val fileName = name?.takeIf { it.isNotBlank() }
                        ?: if (extension.isNullOrBlank()) "attachment" else "attachment.$extension"
                    context.contentResolver.openInputStream(uri).use { input ->
                        checkNotNull(input) { "Shared content is unavailable" }
                        target.outputStream().use { output -> input.copyTo(output) }
                    }
                    files.put(
                        JSONObject().put("path", target.absolutePath)
                            .put("name", fileName)
                            .put("mime", mime ?: "application/octet-stream"),
                    )
                } catch (error: Exception) {
                    target.delete()
                    failed += 1
                    Log.w("EcoPasteShare", "Unable to read shared attachment", error)
                }
            }
            payload.put("files", files)
        } catch (error: Exception) {
            failed += 1
            Log.w("EcoPasteShare", "Unable to read share delivery", error)
        }

        try {
            // Cold-start delivery can arrive before the database and image store exist.
            runtimeReady.get()
            payload.put("failed", failed)
            val message = EcoPasteBridge.receiveShare(payload.toString())
            Handler(Looper.getMainLooper()).post {
                Toast.makeText(context, message, Toast.LENGTH_LONG).show()
            }
        } catch (error: Exception) {
            Log.e("EcoPasteShare", "Unable to import share delivery", error)
        } finally {
            staging?.deleteRecursively()
        }
    }
}
