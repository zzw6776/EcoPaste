package com.ayangweb.eco_paste

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.provider.OpenableColumns
import android.text.Html
import android.util.Log
import android.webkit.MimeTypeMap
import android.widget.Toast
import androidx.core.content.IntentCompat
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import org.json.JSONArray
import org.json.JSONObject

/** Opens temporary Android content grants off the UI thread and lets Rust own materialization. */
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
        val sourcePackage = delivery.getStringExtra(SHARE_SOURCE_PACKAGE)
            ?: (context as? Activity)?.let { shareSourcePackage(it) }
        executor.execute { importDelivery(application, delivery, sourcePackage) }
    }

    /** Keeps source descriptors alive while Rust streams them once into managed storage. */
    private fun importDelivery(context: Context, intent: Intent, sourcePackage: String?) {
        val payload = JSONObject()
        val descriptors = mutableListOf<ParcelFileDescriptor>()
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
            uris.forEach { uri ->
                try {
                    // Only provider-granted content is accepted; never read arbitrary file:// paths.
                    require(uri.scheme == "content")
                    var name: String? = null
                    var size: Long? = null
                    try {
                        context.contentResolver.query(
                            uri,
                            arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE),
                            null,
                            null,
                            null,
                        )?.use { cursor ->
                            if (cursor.moveToFirst()) {
                                val nameColumn = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                                if (nameColumn >= 0) name = cursor.getString(nameColumn)
                                val sizeColumn = cursor.getColumnIndex(OpenableColumns.SIZE)
                                if (sizeColumn >= 0 && !cursor.isNull(sizeColumn)) {
                                    size = cursor.getLong(sizeColumn).takeIf { it >= 0L }
                                }
                            }
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
                    val descriptor = checkNotNull(
                        context.contentResolver.openFileDescriptor(uri, "r"),
                    ) { "Shared content is unavailable" }
                    descriptors.add(descriptor)
                    val resolvedSize = size ?: descriptor.statSize.takeIf { it >= 0L }
                    val file = JSONObject()
                        .put("fd", descriptor.fd)
                        .put("name", fileName)
                        .put("mime", mime ?: "application/octet-stream")
                    resolvedSize?.let { file.put("size", it) }
                    files.put(file)
                } catch (error: Exception) {
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
            payload.put("sourceApp", EcoPasteBridge.shareSourceApp(sourcePackage))
            payload.put("failed", failed)
            val message = EcoPasteBridge.receiveShare(payload.toString())
            Handler(Looper.getMainLooper()).post {
                Toast.makeText(context, message, Toast.LENGTH_LONG).show()
            }
        } catch (error: Exception) {
            Log.e("EcoPasteShare", "Unable to import share delivery", error)
        } finally {
            descriptors.forEach { descriptor ->
                try {
                    descriptor.close()
                } catch (error: Exception) {
                    Log.d("EcoPasteShare", "Unable to close shared attachment", error)
                }
            }
        }
    }
}
