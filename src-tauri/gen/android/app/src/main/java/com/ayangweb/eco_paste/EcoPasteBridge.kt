package com.ayangweb.eco_paste

import android.Manifest
import android.app.Activity
import android.app.ActivityManager
import android.content.ActivityNotFoundException
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.Canvas
import android.net.ConnectivityManager
import android.net.LinkProperties
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.net.Uri
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Environment
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import android.os.PersistableBundle
import android.os.SystemClock
import android.provider.DocumentsContract
import android.provider.Settings
import android.util.Log
import android.util.LruCache
import android.webkit.MimeTypeMap
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileNotFoundException
import java.io.FileInputStream
import java.lang.ref.WeakReference
import java.net.Inet4Address
import java.net.NetworkInterface
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.ZipInputStream
import kotlin.concurrent.thread

object EcoPasteBridge {
    private const val TAG = "EcoPasteBridge"
    private const val FILE_ACTION_TAG = "EcoPasteFileAction"
    private const val PREFS_NAME = "ecopaste_android"
    private const val KEY_ENGINE_MODE = "engine_mode"
    private const val KEY_MODE_SELECTED = "mode_selected"
    private const val KEY_BACKGROUND_KEEP_ALIVE = "background_keep_alive"
    private const val KEY_GESTURE_ENABLED = "gesture_enabled"
    private const val KEY_GESTURE_HIDE_OVERLAY = "gesture_hide_overlay"
    private const val KEY_GESTURE_POPUP_HEIGHT_PERCENT = "gesture_popup_height_percent"
    private const val KEY_GESTURE_LEFT_WIDTH_DP = "gesture_left_width_dp"
    private const val KEY_GESTURE_LEFT_HEIGHT_DP = "gesture_left_height_dp"
    private const val KEY_GESTURE_RIGHT_WIDTH_DP = "gesture_right_width_dp"
    private const val KEY_GESTURE_RIGHT_HEIGHT_DP = "gesture_right_height_dp"
    private const val ROOT_STATUS_UNKNOWN = "unknown"
    private const val ROOT_STATUS_AUTHORIZED = "authorized"
    private const val ROOT_STATUS_UNAVAILABLE = "unavailable"
    private const val CLIPBOARD_WRITEBACK_MARKER = "com.ayangweb.eco_paste.WRITEBACK"
    private const val SOURCE_APP_CACHE_BYTES = 4 * 1024 * 1024
    private var currentActivityRef: WeakReference<Activity>? = null
    private var lanDiscoveryLock: WifiManager.MulticastLock? = null
    private var lanDiscoveryLeaseCount = 0
    private var applicationContext: Context? = null
    private var clipboardManager: ClipboardManager? = null
    private var clipboardListenerRegistered = false
    private var defaultNetworkCallback: ConnectivityManager.NetworkCallback? = null
    private var defaultNetwork: Network? = null
    private var defaultNetworkFingerprint: String? = null
    private var pendingDefaultNetworkNotification: PendingNetworkNotification? = null
    private var lanNetworkCallback: ConnectivityManager.NetworkCallback? = null
    private var lanNetwork: Network? = null
    private var wifiLanFingerprint: String? = null
    private var wifiLanAddresses = emptySet<String>()
    private var tetheredInterfaceNames: Set<String>? = null
    private var tetheringEventCallback: Any? = null
    private var publishedLanFingerprint: String? = null
    private var pendingLanNetworkNotification: PendingNetworkNotification? = null
    private var rootClipboardMonitor: RootClipboardMonitor? = null
    private var foregroundCaptureActive = false
    private val sourceAppCache = object : LruCache<String, SourceAppMetadata>(
        SOURCE_APP_CACHE_BYTES,
    ) {
        override fun sizeOf(key: String, value: SourceAppMetadata): Int {
            return value.iconPng.size + (value.packageName.length + value.appName.length) * 2
        }
    }
    @Volatile
    private var rootAuthorizationStatus = ROOT_STATUS_UNKNOWN
    @Volatile
    private var currentEngineMode: String = "foreground" // "root", "foreground"
    private val remoteDownloadSinks = ConcurrentHashMap<String, RemoteDownloadSink>()
    private val remoteDownloadCommitLock = Any()

    private data class RemoteDownloadSink(
        val uri: Uri,
        val parentUri: Uri,
        val finalName: String,
        val workerCompleted: CountDownLatch? = null,
        val workerError: AtomicReference<String?>? = null,
        val canceled: AtomicBoolean = AtomicBoolean(false),
        val cleanupRequested: AtomicBoolean = AtomicBoolean(false),
        val cleanupStarted: AtomicBoolean = AtomicBoolean(false),
    )

    data class GestureConfig(
        val enabled: Boolean,
        val hideOverlay: Boolean,
        val popupHeightPercent: Int,
        val leftWidthDp: Int,
        val leftHeightDp: Int,
        val rightWidthDp: Int,
        val rightHeightDp: Int,
    )

    private enum class NetworkNotificationKind {
        CHANGED,
        LOST,
    }

    private data class PendingNetworkNotification(
        val kind: NetworkNotificationKind,
        val fingerprint: String? = null,
        val interfaceAddresses: String = "",
    )

    // 剪贴板变更监听回调队列
    private val clipboardChangeListeners = mutableListOf<(String) -> Unit>()
    private var lastCapturedText: String? = null
    private var lastCapturedTimestamp = Long.MIN_VALUE
    private var legacyClipboardSequence = 0L
    private var syncStatusChangedListener: (() -> Unit)? = null
    private var clipboardDataChangedListener: (() -> Unit)? = null
    private val clipChangedListener = ClipboardManager.OnPrimaryClipChangedListener {
        captureClipboardChange(clipboardManager, true)
    }

    @JvmStatic
    external fun initNdkContext(context: Context)

    @JvmStatic
    external fun loadOverlayItemsJson(keyword: String, limit: Int): String

    @JvmStatic
    external fun loadOverlaySyncStatusJson(): String

    @JvmStatic
    external fun loadOverlayCloudRecordsJson(beforeCursor: Long, limit: Int): String

    @JvmStatic
    external fun syncOverlayItemJson(id: String, target: String): String

    @JvmStatic
    external fun reconnectOverlayPeer(deviceId: String): Boolean

    @JvmStatic
    external fun reconnectOverlayCloud(): Boolean

    @JvmStatic
    external fun captureClipboardText(
        text: String,
        packageName: String,
        appName: String,
        iconPng: ByteArray,
    ): Boolean

    @JvmStatic
    external fun pasteOverlayItem(id: String): Boolean

    @JvmStatic
    external fun persistOverlayPanelHeightPercent(heightPercent: Int): Boolean

    @JvmStatic
    external fun notifySyncDefaultNetworkChanged(): Boolean

    @JvmStatic
    external fun notifySyncDefaultNetworkLost(): Boolean

    @JvmStatic
    external fun notifySyncLanNetworkChanged(interfaceAddresses: String): Boolean

    @JvmStatic
    external fun notifySyncLanNetworkLost(): Boolean

    @JvmStatic
    external fun notifySyncStatusRefresh()

    @JvmStatic
    external fun refreshAutomaticDeviceName(name: String)

    @JvmStatic
    fun setSyncStatusChangedListener(listener: (() -> Unit)?) {
        syncStatusChangedListener = listener
    }

    @JvmStatic
    fun onSyncStatusChanged() {
        Handler(Looper.getMainLooper()).post { syncStatusChangedListener?.invoke() }
    }

    @JvmStatic
    fun setClipboardDataChangedListener(listener: (() -> Unit)?) {
        clipboardDataChangedListener = listener
    }

    @JvmStatic
    fun onClipboardDataChanged() {
        Handler(Looper.getMainLooper()).post { clipboardDataChangedListener?.invoke() }
    }

    @JvmStatic
    fun setCurrentActivity(activity: Activity?) {
        currentActivityRef = if (activity != null) WeakReference(activity) else null
    }

    @JvmStatic
    fun getCurrentActivity(): Activity? = currentActivityRef?.get()

    /** Stable logcat channel for end-to-end Android file action diagnostics. */
    @JvmStatic
    fun logFileAction(stage: String, message: String) {
        Log.w(FILE_ACTION_TAG, "$stage | $message")
    }

    /** Opens one validated clipboard file through a temporary read-only content URI. */
    @JvmStatic
    fun openClipboardFile(context: Context, path: String): String {
        logFileAction("native.open.start", "path=$path")
        return try {
            val file = File(path)
            if (!file.isFile) {
                logFileAction("native.open.missing", "exists=${file.exists()} path=$path")
                return fileActionResult("missing")
            }

            val uri = FileProvider.getUriForFile(
                context,
                "${context.packageName}.fileprovider",
                file,
            )
            val resolvedMimeType = mimeType(file)
            logFileAction("native.open.uri", "mime=$resolvedMimeType uri=$uri")
            val viewIntent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri, resolvedMimeType)
                clipData = ClipData.newRawUri(file.name, uri)
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }

            val targetContext = getCurrentActivity() ?: context
            val chooser = Intent.createChooser(viewIntent, null).apply {
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                if (targetContext !is Activity) addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            logFileAction(
                "native.open.launch",
                "context=${targetContext.javaClass.name} flags=${chooser.flags}",
            )
            targetContext.startActivity(chooser)
            logFileAction("native.open.success", "chooser started")
            fileActionResult("success")
        } catch (error: ActivityNotFoundException) {
            Log.w(FILE_ACTION_TAG, "native.open.unavailable | ${error.message}", error)
            fileActionResult("unavailable", error.message.orEmpty())
        } catch (error: Throwable) {
            Log.e(FILE_ACTION_TAG, "native.open.failed | ${error.message}", error)
            fileActionResult("failed", error.message.orEmpty())
        }
    }

    /** Exports one validated clipboard file through Android's Storage Access Framework. */
    @JvmStatic
    fun saveClipboardFile(context: Context, path: String): String {
        logFileAction("native.save.start", "package=${context.packageName} path=$path")
        val file = File(path)
        if (!file.isFile) {
            logFileAction("native.save.missing", "exists=${file.exists()} path=$path")
            return fileActionResult("missing")
        }
        val activity = getCurrentActivity() as? MainActivity
        if (activity == null) {
            logFileAction("native.save.unavailable", "MainActivity is not available")
            return fileActionResult("unavailable")
        }
        val result = AtomicReference(fileActionResult("failed"))
        val completed = CountDownLatch(1)

        Handler(Looper.getMainLooper()).post {
            val resolvedMimeType = mimeType(file)
            logFileAction(
                "native.save.picker",
                "name=${file.name} mime=$resolvedMimeType",
            )
            activity.createDocument(file.name, resolvedMimeType) { targetUri ->
                if (targetUri == null) {
                    logFileAction("native.save.cancelled", "picker returned no URI")
                    result.set(fileActionResult("cancelled"))
                    completed.countDown()
                    return@createDocument
                }
                logFileAction("native.save.copy.start", "target=$targetUri")
                thread(name = "EcoPasteFileExport") {
                    try {
                        FileInputStream(file).use { input ->
                            activity.contentResolver.openOutputStream(targetUri, "w").use { output ->
                                requireNotNull(output) { "destination stream unavailable" }
                                input.copyTo(output)
                            }
                        }
                        logFileAction("native.save.success", "target=$targetUri")
                        result.set(fileActionResult("success"))
                    } catch (error: Throwable) {
                        Log.e(FILE_ACTION_TAG, "native.save.failed | ${error.message}", error)
                        result.set(fileActionResult("failed", error.message.orEmpty()))
                    } finally {
                        completed.countDown()
                    }
                }
            }
        }

        if (!completed.await(10, TimeUnit.MINUTES)) {
            logFileAction("native.save.timeout", "document picker timed out")
            return fileActionResult("failed", "document picker timed out")
        }
        return result.get()
    }

    /** Prompts before a remote download and returns only a Storage Access Framework URI. */
    @JvmStatic
    fun pickRemoteDownloadTarget(
        context: Context,
    ): String {
        val activity = getCurrentActivity() as? MainActivity
            ?: return remoteDownloadTargetResult("unavailable")
        val result = AtomicReference(remoteDownloadTargetResult("failed"))
        val completed = CountDownLatch(1)
        Handler(Looper.getMainLooper()).post {
            val callback: (Uri?) -> Unit = { targetUri ->
                if (targetUri == null) {
                    result.set(remoteDownloadTargetResult("cancelled"))
                } else {
                    try {
                        activity.contentResolver.takePersistableUriPermission(
                            targetUri,
                            Intent.FLAG_GRANT_READ_URI_PERMISSION or
                                Intent.FLAG_GRANT_WRITE_URI_PERMISSION,
                        )
                    } catch (error: SecurityException) {
                        Log.w(FILE_ACTION_TAG, "persist tree permission failed", error)
                    }
                    result.set(
                        remoteDownloadTargetResult(
                            "success",
                            targetUri.toString(),
                        ),
                    )
                }
                completed.countDown()
            }
            activity.openDocumentTree(callback)
        }
        if (!completed.await(10, TimeUnit.MINUTES)) {
            return remoteDownloadTargetResult("failed", message = "document picker timed out")
        }
        return result.get()
    }

    /** Opens a user-selected destination as a raw descriptor for Rust's authenticated stream. */
    @JvmStatic
    fun openRemoteDownloadSink(
        context: Context,
        targetUriValue: String,
        fileName: String,
        mimeType: String,
        directoryArchive: Boolean,
    ): String {
        var temporaryUri: Uri? = null
        var detachedFd: Int? = null
        var registeredToken: String? = null
        var registeredSink: RemoteDownloadSink? = null
        var directoryWorkerStarted = false
        var directoryReadPipe: ParcelFileDescriptor? = null
        var directoryWritePipe: ParcelFileDescriptor? = null
        return try {
            val targetUri = Uri.parse(targetUriValue)
            val finalName = safeRemoteName(fileName)
            val token = UUID.randomUUID().toString()
            val parent = DocumentsContract.buildDocumentUriUsingTree(
                targetUri,
                DocumentsContract.getTreeDocumentId(targetUri),
            )
            val temporaryName = ".${finalName}.ecopaste-${token.take(8)}.part"
            temporaryUri = requireNotNull(
                DocumentsContract.createDocument(
                    context.contentResolver,
                    parent,
                    if (directoryArchive) DocumentsContract.Document.MIME_TYPE_DIR else mimeType,
                    temporaryName,
                ),
            ) { "destination document unavailable" }
            val destinationUri = requireNotNull(temporaryUri)

            if (!directoryArchive) {
                val descriptor = requireNotNull(
                    context.contentResolver.openFileDescriptor(destinationUri, "w"),
                ) { "destination descriptor unavailable" }
                val fd = descriptor.detachFd()
                descriptor.close()
                detachedFd = fd
                val sink = RemoteDownloadSink(destinationUri, parent, finalName)
                remoteDownloadSinks[token] = sink
                registeredToken = token
                registeredSink = sink
                val result = remoteDownloadSinkResult("success", fd, token)
                temporaryUri = null
                registeredToken = null
                registeredSink = null
                detachedFd = null
                return result
            }

            val pipe = ParcelFileDescriptor.createPipe()
            val readPipe = pipe[0]
            val writePipe = pipe[1]
            directoryReadPipe = readPipe
            directoryWritePipe = writePipe
            val completed = CountDownLatch(1)
            val workerError = AtomicReference<String?>(null)
            val sink = RemoteDownloadSink(
                destinationUri,
                parent,
                finalName,
                completed,
                workerError,
            )
            remoteDownloadSinks[token] = sink
            registeredToken = token
            registeredSink = sink
            thread(name = "EcoPasteRemoteDirectory") {
                try {
                    ParcelFileDescriptor.AutoCloseInputStream(readPipe).use { input ->
                        extractArchiveToTree(context, input, destinationUri, sink.canceled)
                    }
                } catch (error: Throwable) {
                    workerError.set(error.message ?: error.javaClass.simpleName)
                    if (!sink.canceled.get()) {
                        Log.e(FILE_ACTION_TAG, "remote directory extraction failed", error)
                    }
                } finally {
                    completed.countDown()
                    if (sink.cleanupRequested.get()) {
                        deleteRemoteDownloadSink(context, sink)
                    }
                }
            }
            directoryWorkerStarted = true
            val fd = writePipe.detachFd()
            detachedFd = fd
            writePipe.close()
            directoryReadPipe = null
            directoryWritePipe = null
            val result = remoteDownloadSinkResult("success", fd, token)
            temporaryUri = null
            registeredToken = null
            registeredSink = null
            detachedFd = null
            result
        } catch (error: Throwable) {
            registeredToken?.let(remoteDownloadSinks::remove)
            detachedFd?.let { fd ->
                try {
                    ParcelFileDescriptor.adoptFd(fd).close()
                } catch (_: Throwable) {
                    // The descriptor may already have been transferred or closed.
                }
            }
            try {
                directoryWritePipe?.close()
            } catch (_: Throwable) {
                // Closing the detached descriptor above is sufficient when ownership moved.
            }
            if (!directoryWorkerStarted) {
                try {
                    directoryReadPipe?.close()
                } catch (_: Throwable) {
                    // The descriptor may already have been closed during failed startup.
                }
            }
            val sink = registeredSink
            if (sink != null) {
                if (sink.workerCompleted != null && !directoryWorkerStarted) {
                    sink.workerCompleted.countDown()
                }
                requestRemoteDownloadCleanup(context, sink)
            } else {
                temporaryUri?.let { uri ->
                    try {
                        DocumentsContract.deleteDocument(context.contentResolver, uri)
                    } catch (_: Throwable) {
                        // The provider may already have removed the temporary document.
                    }
                }
            }
            Log.e(FILE_ACTION_TAG, "open remote download sink failed", error)
            remoteDownloadSinkResult("failed", message = error.message.orEmpty())
        }
    }

    /** Commits an atomic tree download or removes every partial destination on failure. */
    @JvmStatic
    fun finishRemoteDownloadSink(context: Context, token: String, success: Boolean): String {
        val sink = remoteDownloadSinks[token]
            ?: return fileActionResult("failed", "download destination is no longer available")
        return try {
            val workerFinished = sink.workerCompleted?.await(10, TimeUnit.MINUTES) ?: true
            val canceled = !success || sink.canceled.get()
            if (!workerFinished) {
                requestRemoteDownloadCleanup(context, sink)
                return fileActionResult(
                    if (canceled) "cancelled" else "failed",
                    if (canceled) "" else "directory extraction timed out",
                )
            }
            val workerError = sink.workerError?.get()
            if (canceled || workerError != null) {
                requestRemoteDownloadCleanup(context, sink)
                return fileActionResult(
                    if (canceled) "cancelled" else "failed",
                    workerError.orEmpty(),
                )
            }
            val committedUri = synchronized(remoteDownloadCommitLock) {
                val finalName = nonConflictingRemoteName(context, sink.parentUri, sink.finalName)
                requireNotNull(
                    DocumentsContract.renameDocument(
                        context.contentResolver,
                        sink.uri,
                        finalName,
                    ),
                ) { "unable to finalize destination document" }
            }
            fileActionResult("success", uri = committedUri.toString())
        } catch (error: Throwable) {
            requestRemoteDownloadCleanup(context, sink)
            Log.e(FILE_ACTION_TAG, "finish remote download sink failed", error)
            fileActionResult("failed", error.message.orEmpty())
        } finally {
            remoteDownloadSinks.remove(token, sink)
        }
    }

    /** Defers deletion until a directory extraction worker can no longer write to the tree. */
    private fun requestRemoteDownloadCleanup(context: Context, sink: RemoteDownloadSink) {
        sink.canceled.set(true)
        sink.cleanupRequested.set(true)
        if (sink.workerCompleted == null || sink.workerCompleted.count == 0L) {
            deleteRemoteDownloadSink(context, sink)
        }
    }

    private fun deleteRemoteDownloadSink(context: Context, sink: RemoteDownloadSink) {
        if (!sink.cleanupStarted.compareAndSet(false, true)) return
        try {
            DocumentsContract.deleteDocument(context.contentResolver, sink.uri)
        } catch (_: FileNotFoundException) {
            // The provider already removed the temporary document.
        } catch (error: Throwable) {
            Log.w(FILE_ACTION_TAG, "delete partial remote download failed", error)
        }
    }

    /** Matches desktop download behavior by preserving existing entries and numbering duplicates. */
    private fun nonConflictingRemoteName(
        context: Context,
        parentUri: Uri,
        requestedName: String,
    ): String {
        val childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(
            parentUri,
            DocumentsContract.getDocumentId(parentUri),
        )
        val existingNames = mutableSetOf<String>()
        context.contentResolver.query(
            childrenUri,
            arrayOf(DocumentsContract.Document.COLUMN_DISPLAY_NAME),
            null,
            null,
            null,
        )?.use { cursor ->
            val nameColumn = cursor.getColumnIndexOrThrow(
                DocumentsContract.Document.COLUMN_DISPLAY_NAME,
            )
            while (cursor.moveToNext()) {
                existingNames += cursor.getString(nameColumn).lowercase(Locale.ROOT)
            }
        }
        if (requestedName.lowercase(Locale.ROOT) !in existingNames) return requestedName

        val extensionIndex = requestedName.lastIndexOf('.').takeIf { index ->
            index > 0 && index < requestedName.lastIndex
        }
        val stem = extensionIndex?.let { requestedName.substring(0, it) } ?: requestedName
        val extension = extensionIndex?.let { requestedName.substring(it) }.orEmpty()
        for (index in 1..999) {
            val candidate = "$stem ($index)$extension"
            if (candidate.lowercase(Locale.ROOT) !in existingNames) return candidate
        }
        return "$stem-${UUID.randomUUID().toString().take(8)}$extension"
    }

    /** Signals a directory extraction worker without waiting for the blocking finalizer. */
    @JvmStatic
    fun cancelRemoteDownloadSink(token: String): Boolean {
        val sink = remoteDownloadSinks[token] ?: return false
        sink.canceled.set(true)
        return true
    }

    /** Removes one completed child document when a multi-file batch is rolled back. */
    @JvmStatic
    fun discardRemoteDownloadDocument(context: Context, uriValue: String): String {
        return try {
            DocumentsContract.deleteDocument(context.contentResolver, Uri.parse(uriValue))
            fileActionResult("success")
        } catch (_: FileNotFoundException) {
            fileActionResult("success")
        } catch (error: Throwable) {
            Log.e(FILE_ACTION_TAG, "discard remote download document failed", error)
            fileActionResult("failed", error.message.orEmpty())
        }
    }

    /** Checks one previously committed SAF document without scanning user storage. */
    @JvmStatic
    fun isRemoteDownloadDocumentAvailable(
        context: Context,
        uriValue: String,
        expectedSize: Long,
        directory: Boolean,
    ): Boolean {
        return try {
            val uri = Uri.parse(uriValue)
            val projection = arrayOf(
                DocumentsContract.Document.COLUMN_MIME_TYPE,
                DocumentsContract.Document.COLUMN_SIZE,
            )
            context.contentResolver.query(uri, projection, null, null, null)?.use { cursor ->
                if (!cursor.moveToFirst()) return false
                val mimeIndex = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_MIME_TYPE)
                val mimeType = if (mimeIndex >= 0 && !cursor.isNull(mimeIndex)) {
                    cursor.getString(mimeIndex)
                } else {
                    null
                }
                val isDirectory = mimeType == DocumentsContract.Document.MIME_TYPE_DIR
                if (directory) return isDirectory
                if (isDirectory) return false

                val sizeIndex = cursor.getColumnIndex(DocumentsContract.Document.COLUMN_SIZE)
                if (sizeIndex >= 0 && !cursor.isNull(sizeIndex)) {
                    return cursor.getLong(sizeIndex) == expectedSize
                }
            } ?: return false

            context.contentResolver.openFileDescriptor(uri, "r")?.use { descriptor ->
                descriptor.statSize < 0 || descriptor.statSize == expectedSize
            } ?: false
        } catch (_: Throwable) {
            false
        }
    }

    /** Opens the system package installer; Android still requires explicit user confirmation. */
    @JvmStatic
    fun openPackageInstaller(context: Context, path: String): String {
        val file = File(path)
        if (!file.isFile) return fileActionResult("missing", "update APK does not exist")

        val result = AtomicReference(fileActionResult("failed"))
        val completed = CountDownLatch(1)
        Handler(Looper.getMainLooper()).post {
            try {
                val uri = FileProvider.getUriForFile(
                    context,
                    "${context.packageName}.fileprovider",
                    file,
                )
                val installIntent = Intent(Intent.ACTION_VIEW).apply {
                    setDataAndType(uri, "application/vnd.android.package-archive")
                    clipData = ClipData.newRawUri(file.name, uri)
                    addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                }
                val targetContext = getCurrentActivity() ?: context
                if (targetContext !is Activity) {
                    installIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                targetContext.startActivity(installIntent)
                result.set(fileActionResult("success"))
            } catch (error: ActivityNotFoundException) {
                result.set(fileActionResult("unavailable", error.message.orEmpty()))
            } catch (error: Throwable) {
                Log.e(TAG, "open package installer failed", error)
                result.set(fileActionResult("failed", error.message.orEmpty()))
            } finally {
                completed.countDown()
            }
        }
        if (!completed.await(10, TimeUnit.SECONDS)) {
            return fileActionResult("failed", "package installer launch timed out")
        }
        return result.get()
    }

    private fun mimeType(file: File): String {
        val extension = file.extension.lowercase()
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)
            ?: "application/octet-stream"
    }

    private fun mimeType(fileName: String): String {
        val extension = fileName.substringAfterLast('.', "").lowercase()
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)
            ?: "application/octet-stream"
    }

    private fun safeRemoteName(value: String): String {
        val sanitized = value.map { character ->
            if (character.isISOControl() || character == '/' || character == '\\') '_' else character
        }.joinToString("").trim().trimEnd('.')
        return sanitized.ifEmpty { "download" }
    }

    private fun extractArchiveToTree(
        context: Context,
        input: java.io.InputStream,
        root: Uri,
        canceled: AtomicBoolean,
    ) {
        val directories = mutableMapOf("" to root)
        val created = mutableSetOf<String>()
        ZipInputStream(input.buffered()).use { archive ->
            while (true) {
                check(!canceled.get()) { "transfer canceled" }
                val entry = archive.nextEntry ?: break
                val normalized = entry.name.replace('\\', '/')
                val segments = normalized.split('/').filter { it.isNotEmpty() }
                require(
                    segments.isNotEmpty() && segments.none { it == "." || it == ".." },
                ) { "directory archive contains an unsafe path" }
                var parentPath = ""
                var parentUri = root
                val directoryCount = if (entry.isDirectory) segments.size else segments.size - 1
                for (index in 0 until directoryCount) {
                    check(!canceled.get()) { "transfer canceled" }
                    val segment = safeRemoteName(segments[index])
                    val path = if (parentPath.isEmpty()) segment else "$parentPath/$segment"
                    parentUri = directories.getOrPut(path) {
                        requireNotNull(
                            DocumentsContract.createDocument(
                                context.contentResolver,
                                parentUri,
                                DocumentsContract.Document.MIME_TYPE_DIR,
                                segment,
                            ),
                        ) { "unable to create destination directory" }
                    }
                    parentPath = path
                }
                if (!entry.isDirectory) {
                    check(!canceled.get()) { "transfer canceled" }
                    val name = safeRemoteName(segments.last())
                    val path = if (parentPath.isEmpty()) name else "$parentPath/$name"
                    require(created.add(path)) { "directory archive contains duplicate entries" }
                    val fileUri = requireNotNull(
                        DocumentsContract.createDocument(
                            context.contentResolver,
                            parentUri,
                            mimeType(name),
                            name,
                        ),
                    ) { "unable to create destination file" }
                    context.contentResolver.openOutputStream(fileUri, "w").use { output ->
                        requireNotNull(output) { "destination stream unavailable" }
                        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                        while (true) {
                            check(!canceled.get()) { "transfer canceled" }
                            val read = archive.read(buffer)
                            if (read < 0) break
                            output.write(buffer, 0, read)
                        }
                    }
                }
                archive.closeEntry()
            }
        }
    }

    private fun remoteDownloadTargetResult(
        status: String,
        uri: String = "",
        message: String = "",
    ): String {
        return JSONObject().apply {
            put("status", status)
            put("uri", uri)
            put("message", message)
        }.toString()
    }

    private fun remoteDownloadSinkResult(
        status: String,
        fd: Int = -1,
        token: String = "",
        message: String = "",
    ): String {
        return JSONObject().apply {
            put("status", status)
            put("fd", fd)
            put("token", token)
            put("message", message)
        }.toString()
    }

    private fun fileActionResult(
        status: String,
        message: String = "",
        uri: String = "",
    ): String {
        return JSONObject().apply {
            put("status", status)
            put("message", message)
            put("uri", uri)
        }.toString()
    }

    @JvmStatic
    fun initialize(context: Context) {
        applicationContext = context.applicationContext
        val preferences = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val storedMode = preferences.getString(KEY_ENGINE_MODE, "foreground")
        currentEngineMode = if (storedMode == "root") "root" else "foreground"
        if (storedMode != currentEngineMode) {
            preferences.edit().putString(KEY_ENGINE_MODE, currentEngineMode).apply()
        }
        refreshClipboardListener()
        registerSyncNetworkCallbacks(context.applicationContext)
        refreshLanInterfaceNotification("initialize")
        replayPendingSyncNetworkNotifications()
        if (isShareRuntimeReady()) EcoPasteShareReceiver.onRuntimeReady()
    }

    /** Replays callbacks that raced with creation of the native synchronization manager. */
    @JvmStatic
    fun onSyncRuntimeReady() {
        EcoPasteShareReceiver.onRuntimeReady()
        replayPendingSyncNetworkNotifications()
    }

    @JvmStatic
    external fun receiveShare(payload: String): String

    @JvmStatic
    external fun isShareRuntimeReady(): Boolean

    /** Separates the app's cloud default route from Wi-Fi-only LAN discovery lifecycle. */
    @Synchronized
    private fun registerSyncNetworkCallbacks(context: Context) {
        val connectivityManager = context.getSystemService(Context.CONNECTIVITY_SERVICE)
            as? ConnectivityManager ?: return

        if (defaultNetworkCallback == null) {
            registerDefaultNetworkCallback(connectivityManager)
        }
        if (lanNetworkCallback == null) {
            registerLanNetworkCallback(connectivityManager)
        }
        if (
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.BAKLAVA &&
            tetheringEventCallback == null
        ) {
            registerTetheringEventCallback(context)
        }
    }

    /** Tracks the network Android selected for this app's cloud traffic. */
    private fun registerDefaultNetworkCallback(connectivityManager: ConnectivityManager) {
        connectivityManager.activeNetwork?.let { network ->
            val properties = connectivityManager.getLinkProperties(network)
            val capabilities = connectivityManager.getNetworkCapabilities(network)
            synchronized(this) {
                defaultNetwork = network
                if (properties != null && capabilities != null) {
                    defaultNetworkFingerprint = networkFingerprint(
                        network,
                        properties,
                        capabilities,
                    )
                }
            }
        }
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                synchronized(this@EcoPasteBridge) {
                    defaultNetwork = network
                }
                notifyDefaultNetworkChanged(connectivityManager, network, "available")
            }

            override fun onLinkPropertiesChanged(network: Network, properties: LinkProperties) {
                if (!isCurrentDefaultNetwork(network)) return
                notifyDefaultNetworkChanged(
                    connectivityManager,
                    network,
                    "link-properties",
                    properties,
                )
            }

            override fun onCapabilitiesChanged(
                network: Network,
                capabilities: NetworkCapabilities,
            ) {
                if (!isCurrentDefaultNetwork(network)) return
                notifyDefaultNetworkChanged(
                    connectivityManager,
                    network,
                    "capabilities",
                    capabilities = capabilities,
                )
            }

            override fun onLost(network: Network) {
                synchronized(this@EcoPasteBridge) {
                    if (defaultNetwork != network) return
                    defaultNetwork = null
                    defaultNetworkFingerprint = null
                    queueDefaultNetworkNotification(
                        PendingNetworkNotification(NetworkNotificationKind.LOST),
                        "lost:$network",
                    )
                }
            }
        }
        try {
            connectivityManager.registerDefaultNetworkCallback(callback)
            defaultNetworkCallback = callback
        } catch (error: RuntimeException) {
            Log.w(TAG, "register default sync network callback failed: ${error.message}")
        }
    }

    /** Tracks Wi-Fi independently because only Wi-Fi interfaces may carry LAN discovery. */
    private fun registerLanNetworkCallback(connectivityManager: ConnectivityManager) {
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                synchronized(this@EcoPasteBridge) {
                    lanNetwork = network
                }
                notifyLanNetworkChanged(connectivityManager, network, "available")
            }

            override fun onLinkPropertiesChanged(network: Network, properties: LinkProperties) {
                if (!isCurrentLanNetwork(network)) return
                notifyLanNetworkChanged(connectivityManager, network, "link-properties", properties)
            }

            override fun onLost(network: Network) {
                synchronized(this@EcoPasteBridge) {
                    if (lanNetwork != network) return
                    lanNetwork = null
                    wifiLanFingerprint = null
                    wifiLanAddresses = emptySet()
                    publishLanInterfaceNotification("wifi-lost:$network")
                }
            }
        }
        val request = NetworkRequest.Builder()
            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
            .build()
        try {
            connectivityManager.registerNetworkCallback(request, callback)
            lanNetworkCallback = callback
        } catch (error: RuntimeException) {
            Log.w(TAG, "register LAN sync network callback failed: ${error.message}")
        }
    }

    /** Tracks hotspot interfaces that Android does not expose as Wi-Fi Network objects. */
    private fun registerTetheringEventCallback(context: Context) {
        try {
            tetheringEventCallback = TetheringMonitorApi36.register(context) { interfaceNames ->
                synchronized(this@EcoPasteBridge) {
                    tetheredInterfaceNames = interfaceNames
                    publishLanInterfaceNotification("tethering-changed")
                }
            }
        } catch (error: RuntimeException) {
            Log.w(TAG, "register tethering callback failed: ${error.message}")
        }
    }

    /** Coalesces duplicate default-network callbacks without hiding effective path changes. */
    @Synchronized
    private fun notifyDefaultNetworkChanged(
        connectivityManager: ConnectivityManager,
        network: Network,
        reason: String,
        properties: LinkProperties? = null,
        capabilities: NetworkCapabilities? = null,
    ) {
        val currentProperties = properties ?: connectivityManager.getLinkProperties(network)
            ?: return
        val currentCapabilities = capabilities
            ?: connectivityManager.getNetworkCapabilities(network)
            ?: return
        val fingerprint = networkFingerprint(network, currentProperties, currentCapabilities)
        if (defaultNetwork != network || defaultNetworkFingerprint == fingerprint) return
        defaultNetworkFingerprint = fingerprint
        Log.i(TAG, "default sync network changed ($reason): $fingerprint")
        queueDefaultNetworkNotification(
            PendingNetworkNotification(
                kind = NetworkNotificationKind.CHANGED,
                fingerprint = fingerprint,
            ),
            "$reason:$fingerprint",
        )
    }

    /** Coalesces duplicate Wi-Fi callbacks and requires at least one usable interface address. */
    @Synchronized
    private fun notifyLanNetworkChanged(
        connectivityManager: ConnectivityManager,
        network: Network,
        reason: String,
        properties: LinkProperties? = null,
    ) {
        val currentProperties = properties ?: connectivityManager.getLinkProperties(network)
            ?: return
        val multicastInterfaces = currentProperties.linkAddresses
            .map { it.address }
            .filterIsInstance<Inet4Address>()
            .map { it.hostAddress.orEmpty() }
            .filter { it.isNotEmpty() }
            .sorted()
        if (multicastInterfaces.isEmpty()) return

        val fingerprint = listOf(
            network.toString(),
            currentProperties.interfaceName.orEmpty(),
            multicastInterfaces.joinToString(","),
        ).joinToString(":")
        if (lanNetwork != network) return
        if (wifiLanFingerprint != fingerprint) {
            wifiLanFingerprint = fingerprint
            wifiLanAddresses = multicastInterfaces.toSet()
        }
        publishLanInterfaceNotification("wifi-$reason")
    }

    /** Publishes the union of client Wi-Fi and hotspot interfaces without background polling. */
    @Synchronized
    private fun refreshLanInterfaceNotification(reason: String) {
        val connectivityManager = applicationContext
            ?.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
        val network = lanNetwork
        if (connectivityManager != null && network != null) {
            val properties = connectivityManager.getLinkProperties(network)
            if (properties != null) {
                notifyLanNetworkChanged(connectivityManager, network, reason, properties)
                return
            }
        }
        publishLanInterfaceNotification(reason)
    }

    /** Sends only meaningful interface-set changes to the Rust synchronization runtime. */
    @Synchronized
    private fun publishLanInterfaceNotification(reason: String) {
        val hotspotAddresses = collectHotspotInterfaceAddresses(tetheredInterfaceNames)
        val multicastInterfaces = (wifiLanAddresses + hotspotAddresses).sorted()
        if (multicastInterfaces.isEmpty()) {
            if (publishedLanFingerprint == null) return
            publishedLanFingerprint = null
            Log.i(TAG, "LAN sync network lost ($reason)")
            queueLanNetworkNotification(
                PendingNetworkNotification(NetworkNotificationKind.LOST),
                reason,
            )
            return
        }

        val fingerprint = listOf(
            wifiLanFingerprint.orEmpty(),
            tetheredInterfaceNames.orEmpty().sorted().joinToString(","),
            multicastInterfaces.joinToString(","),
        ).joinToString(":")
        if (publishedLanFingerprint == fingerprint) return
        publishedLanFingerprint = fingerprint
        Log.i(TAG, "LAN sync interfaces changed ($reason): $fingerprint")
        queueLanNetworkNotification(
            PendingNetworkNotification(
                kind = NetworkNotificationKind.CHANGED,
                fingerprint = fingerprint,
                interfaceAddresses = multicastInterfaces.joinToString(","),
            ),
            "$reason:$fingerprint",
        )
    }

    /** Resolves private IPv4 addresses from exact tethering names or conservative legacy names. */
    private fun collectHotspotInterfaceAddresses(
        exactInterfaceNames: Set<String>?,
    ): Set<String> {
        return try {
            val interfaces = NetworkInterface.getNetworkInterfaces()?.toList().orEmpty()
            interfaces
                .filter { networkInterface ->
                    val selected = exactInterfaceNames?.contains(networkInterface.name)
                        ?: isLikelyHotspotInterfaceName(networkInterface.name)
                    selected && networkInterface.isUp && !networkInterface.isLoopback &&
                        !networkInterface.isVirtual && networkInterface.supportsMulticast()
                }
                .flatMap { it.inetAddresses.toList() }
                .filterIsInstance<Inet4Address>()
                .filter { !it.isLoopbackAddress && (it.isSiteLocalAddress || it.isLinkLocalAddress) }
                .mapNotNull { it.hostAddress }
                .toSet()
        } catch (error: Exception) {
            Log.d(TAG, "read hotspot interfaces failed: ${error.message}")
            emptySet()
        }
    }

    /** Includes only properties that change the effective outbound QUIC path. */
    private fun networkFingerprint(
        network: Network,
        properties: LinkProperties,
        capabilities: NetworkCapabilities,
    ): String {
        val addresses = properties.linkAddresses
            .map { it.address }
            .filterIsInstance<Inet4Address>()
            .map { it.hostAddress.orEmpty() }
            .filter { it.isNotEmpty() }
            .sorted()
        val routes = properties.routes
            .filter { it.destination.address is Inet4Address }
            .map { route ->
                "${route.destination}:${route.gateway?.hostAddress.orEmpty()}"
            }
            .sorted()
        val transports = listOf(
            NetworkCapabilities.TRANSPORT_CELLULAR to "cellular",
            NetworkCapabilities.TRANSPORT_WIFI to "wifi",
            NetworkCapabilities.TRANSPORT_BLUETOOTH to "bluetooth",
            NetworkCapabilities.TRANSPORT_ETHERNET to "ethernet",
            NetworkCapabilities.TRANSPORT_VPN to "vpn",
        ).filter { (transport, _) -> capabilities.hasTransport(transport) }
            .joinToString(",") { (_, label) -> label }

        return listOf(
            network.toString(),
            properties.interfaceName.orEmpty(),
            transports,
            addresses.joinToString(","),
            routes.joinToString(","),
        ).joinToString(":")
    }

    @Synchronized
    private fun isCurrentDefaultNetwork(network: Network): Boolean {
        return defaultNetwork == network
    }

    @Synchronized
    private fun isCurrentLanNetwork(network: Network): Boolean {
        return lanNetwork == network
    }

    /** Retains the latest callback until the native synchronization manager confirms receipt. */
    @Synchronized
    private fun queueDefaultNetworkNotification(
        notification: PendingNetworkNotification,
        reason: String,
    ) {
        pendingDefaultNetworkNotification = notification
        deliverDefaultNetworkNotification(notification, reason)
    }

    @Synchronized
    private fun queueLanNetworkNotification(
        notification: PendingNetworkNotification,
        reason: String,
    ) {
        pendingLanNetworkNotification = notification
        deliverLanNetworkNotification(notification, reason)
    }

    /** Replays only callbacks that arrived before the Tauri synchronization runtime was ready. */
    @Synchronized
    private fun replayPendingSyncNetworkNotifications() {
        pendingDefaultNetworkNotification?.let { notification ->
            deliverDefaultNetworkNotification(notification, "runtime-ready-replay")
        }
        pendingLanNetworkNotification?.let { notification ->
            deliverLanNetworkNotification(notification, "runtime-ready-replay")
        }
    }

    @Synchronized
    private fun deliverDefaultNetworkNotification(
        notification: PendingNetworkNotification,
        reason: String,
    ) {
        val delivered = try {
            when (notification.kind) {
                NetworkNotificationKind.CHANGED -> notifySyncDefaultNetworkChanged()
                NetworkNotificationKind.LOST -> notifySyncDefaultNetworkLost()
            }
        } catch (error: Throwable) {
            Log.w(TAG, "notify default sync network failed ($reason): ${error.message}")
            false
        }
        if (!delivered) {
            Log.d(TAG, "default sync network notification pending ($reason)")
            return
        }
        if (pendingDefaultNetworkNotification == notification) {
            pendingDefaultNetworkNotification = null
        }
    }

    @Synchronized
    private fun deliverLanNetworkNotification(
        notification: PendingNetworkNotification,
        reason: String,
    ) {
        val delivered = try {
            when (notification.kind) {
                NetworkNotificationKind.CHANGED ->
                    notifySyncLanNetworkChanged(notification.interfaceAddresses)
                NetworkNotificationKind.LOST -> notifySyncLanNetworkLost()
            }
        } catch (error: Throwable) {
            Log.w(TAG, "notify LAN sync network failed ($reason): ${error.message}")
            false
        }
        if (!delivered) {
            Log.d(TAG, "LAN sync network notification pending ($reason)")
            return
        }
        if (pendingLanNetworkNotification == notification) {
            pendingLanNetworkNotification = null
        }
    }

    /** Foreground mode listens only while EcoPaste owns a visible activity. */
    @JvmStatic
    @Synchronized
    fun setForegroundCaptureActive(active: Boolean) {
        foregroundCaptureActive = active
        refreshClipboardListener()
        if (active) {
            if (currentEngineMode == "root") {
                rootClipboardMonitor?.requestCapture()
            } else if (currentEngineMode == "foreground") {
                captureClipboardChange(clipboardManager, false)
            }
        }
    }

    /** Android 默认过滤组播；仅在同步端点运行期间允许接收局域网发现报文。 */
    @JvmStatic
    @Synchronized
    fun setLanDiscoveryEnabled(context: Context, enabled: Boolean) {
        if (!enabled) {
            lanDiscoveryLeaseCount = (lanDiscoveryLeaseCount - 1).coerceAtLeast(0)
            if (lanDiscoveryLeaseCount > 0) return
            lanDiscoveryLock?.takeIf { it.isHeld }?.release()
            lanDiscoveryLock = null
            return
        }
        refreshLanInterfaceNotification("discovery-enabled")
        lanDiscoveryLeaseCount += 1
        if (lanDiscoveryLock?.isHeld == true) return

        val wifiManager = context.applicationContext
            .getSystemService(Context.WIFI_SERVICE) as? WifiManager ?: return
        lanDiscoveryLock = wifiManager.createMulticastLock("EcoPasteLanDiscovery").apply {
            setReferenceCounted(false)
            acquire()
        }
    }

    /** 返回 Android 设置中用户可见的设备名称，未配置时才回退到厂商和型号。 */
    @JvmStatic
    fun getDeviceName(context: Context): String {
        val configuredName = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N_MR1) {
            Settings.Global.getString(context.contentResolver, Settings.Global.DEVICE_NAME)
        } else {
            null
        }
        if (!configuredName.isNullOrBlank()) {
            return configuredName.trim()
        }

        return getDeviceFallbackName()
    }

    /** 返回旧版本使用过的厂商与硬件型号组合，用于修正已持久化的自动名称。 */
    @JvmStatic
    fun getDeviceFallbackName(): String {
        val manufacturer = Build.MANUFACTURER.trim()
        val model = getDeviceModel()
        return if (manufacturer.isEmpty() || model.startsWith(manufacturer, ignoreCase = true)) {
            model
        } else {
            "$manufacturer $model"
        }
    }

    /** 返回设备硬件描述，仅用于识别旧版本自动生成的型号名称。 */
    @JvmStatic
    fun getDeviceModel(): String {
        return Build.MODEL.trim()
    }

    /**
     * 检查所有权限状态并返回 JSON 字符串
     */
    @JvmStatic
    fun getPermissionsJson(context: Context): String {
        val preferences = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val overlayGranted = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Settings.canDrawOverlays(context)
        } else {
            true
        }

        val notificationGranted = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            ContextCompat.checkSelfPermission(
                context,
                Manifest.permission.POST_NOTIFICATIONS
            ) == PackageManager.PERMISSION_GRANTED
        } else {
            true
        }

        val batteryIgnored = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            val pm = context.getSystemService(Context.POWER_SERVICE) as? PowerManager
            pm?.isIgnoringBatteryOptimizations(context.packageName) ?: false
        } else {
            true
        }

        val storageGranted = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            Environment.isExternalStorageManager()
        } else {
            true
        }

        val overlayRunning = isOverlayServiceRunning(context)
        val clipboardMonitorRunning = rootClipboardMonitor?.isReady() == true
        val gestureMonitorRunning = EcoPasteOverlayService.isGestureMonitorReady()
        if (clipboardMonitorRunning || gestureMonitorRunning) {
            rootAuthorizationStatus = ROOT_STATUS_AUTHORIZED
        }
        val mode = if (currentEngineMode == "root") "full" else "basic"
        val modeSelected = preferences.getBoolean(
            KEY_MODE_SELECTED,
            currentEngineMode == "root",
        )

        val json = JSONObject().apply {
            put("overlayGranted", overlayGranted)
            put("notificationGranted", notificationGranted)
            put("batteryIgnored", batteryIgnored)
            put("storageGranted", storageGranted)
            put("rootStatus", rootAuthorizationStatus)
            put("overlayServiceRunning", overlayRunning)
            put("clipboardMonitorRunning", clipboardMonitorRunning)
            put("gestureMonitorRunning", gestureMonitorRunning)
            put(
                "foregroundCaptureRunning",
                currentEngineMode == "foreground" && foregroundCaptureActive &&
                    clipboardListenerRegistered,
            )
            put("mode", mode)
            put("modeSelected", modeSelected)
        }
        return json.toString()
    }

    /**
     * 根据名称请求权限或跳转设置（保证在主线程执行）
     */
    @JvmStatic
    fun requestPermissionByName(context: Context, kind: String) {
        val targetContext = getCurrentActivity() ?: context
        Handler(Looper.getMainLooper()).post {
            try {
                Log.d(TAG, "requestPermissionByName: kind=$kind")
                when (kind) {
                    "overlay" -> requestOverlayPermission(targetContext)
                    "battery" -> requestBatteryOptimization(targetContext)
                    "storage" -> requestStoragePermission(targetContext)
                    "notification" -> {
                        val act = getCurrentActivity()
                        if (act != null && Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                            requestNotificationPermission(act)
                        } else {
                            openAppNotificationSettings(targetContext)
                        }
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "requestPermissionByName failed: ${e.message}", e)
            }
        }
    }

    /**
     * 请求悬浮窗权限
     */
    @JvmStatic
    fun requestOverlayPermission(context: Context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            try {
                val intent = Intent(
                    Settings.ACTION_MANAGE_OVERLAY_PERMISSION,
                    Uri.parse("package:${context.packageName}")
                ).apply {
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                context.startActivity(intent)
            } catch (e: Exception) {
                try {
                    val genericIntent = Intent(Settings.ACTION_MANAGE_OVERLAY_PERMISSION).apply {
                        addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    }
                    context.startActivity(genericIntent)
                } catch (e2: Exception) {
                    openAppDetailsSettings(context)
                }
            }
        }
    }

    /**
     * 请求忽略电池优化（加入白名单）
     */
    @JvmStatic
    fun requestBatteryOptimization(context: Context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            try {
                val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
                    data = Uri.parse("package:${context.packageName}")
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                context.startActivity(intent)
            } catch (e: Exception) {
                try {
                    val fallbackIntent = Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS).apply {
                        addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    }
                    context.startActivity(fallbackIntent)
                } catch (e2: Exception) {
                    openAppDetailsSettings(context)
                }
            }
        } else {
            openAppDetailsSettings(context)
        }
    }

    /** Opens the app-specific all-files access page on Android 11 and newer. */
    @JvmStatic
    fun requestStoragePermission(context: Context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return

        try {
            val intent = Intent(
                Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION,
                Uri.parse("package:${context.packageName}"),
            ).apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            context.startActivity(intent)
        } catch (_: Exception) {
            try {
                val fallbackIntent = Intent(Settings.ACTION_MANAGE_ALL_FILES_ACCESS_PERMISSION).apply {
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                context.startActivity(fallbackIntent)
            } catch (_: Exception) {
                openAppDetailsSettings(context)
            }
        }
    }

    /**
     * 请求通知权限
     */
    @JvmStatic
    fun requestNotificationPermission(activity: Activity) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            ActivityCompat.requestPermissions(
                activity,
                arrayOf(Manifest.permission.POST_NOTIFICATIONS),
                1001
            )
        } else {
            openAppNotificationSettings(activity)
        }
    }

    /**
     * 打开应用系统通知设置页
     */
    @JvmStatic
    fun openAppNotificationSettings(context: Context) {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                val intent = Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).apply {
                    putExtra(Settings.EXTRA_APP_PACKAGE, context.packageName)
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                context.startActivity(intent)
            } else {
                openAppDetailsSettings(context)
            }
        } catch (e: Exception) {
            openAppDetailsSettings(context)
        }
    }

    /**
     * 打开应用详情设置页（终极兜底）
     */
    @JvmStatic
    fun openAppDetailsSettings(context: Context) {
        try {
            val intent = Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS).apply {
                data = Uri.parse("package:${context.packageName}")
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            context.startActivity(intent)
        } catch (e: Exception) {
            Log.e(TAG, "openAppDetailsSettings failed: ${e.message}")
        }
    }

    private fun startOverlayService(context: Context) {
        val keepAlive = isBackgroundKeepAliveEnabled(context)
        val intent = Intent(context, EcoPasteOverlayService::class.java)
            .putExtra(EcoPasteOverlayService.EXTRA_FOREGROUND_REQUESTED, keepAlive)
        try {
            if (keepAlive && Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        } catch (e: Exception) {
            Log.e(TAG, "start overlay service failed: ${e.message}")
        }
    }

    private fun stopOverlayService(context: Context) {
        val intent = Intent(context, EcoPasteOverlayService::class.java)
        try {
            context.stopService(intent)
        } catch (e: Exception) {
            Log.e(TAG, "stop overlay service failed: ${e.message}")
        }
    }

    /** 判断 Root 手势监控服务是否正在运行。 */
    @JvmStatic
    fun isOverlayServiceRunning(context: Context): Boolean {
        val am = context.getSystemService(Context.ACTIVITY_SERVICE) as? ActivityManager ?: return false
        @Suppress("DEPRECATION")
        for (service in am.getRunningServices(Int.MAX_VALUE)) {
            if (EcoPasteOverlayService::class.java.name == service.service.className) {
                return true
            }
        }
        return false
    }

    /**
     * 执行模拟自动粘贴
     */
    @JvmStatic
    fun triggerAutoPaste(): Boolean {
        if (currentEngineMode != "root") return false

        return runRootInputCommand("input keyevent 279") ||
            runRootInputCommand("input keycombination 113 50")
    }

    /** Root 模式直接向仍持有焦点的原应用输入框注入系统粘贴键。 */
    private fun runRootInputCommand(command: String): Boolean {
        return try {
            val process = ProcessBuilder("su", "-c", command)
                .redirectErrorStream(true)
                .start()
            val completed = process.waitFor(2, TimeUnit.SECONDS)
            if (!completed) {
                process.destroy()
                Log.w(TAG, "root paste command timed out: $command")
                false
            } else {
                val success = process.exitValue() == 0
                if (!success) {
                    Log.w(TAG, "root paste command failed: $command")
                }
                success
            }
        } catch (error: Exception) {
            Log.w(TAG, "root paste command error: ${error.message}")
            false
        }
    }

    /**
     * 最小化退回后台（不杀进程）
     */
    @JvmStatic
    fun minimizeCurrentApp(context: Context) {
        val act = getCurrentActivity()
        if (act is MainActivity) {
            act.minimizeToBackground()
        } else if (act != null) {
            act.moveTaskToBack(true)
        }
    }

    /** 用户明确点击授权后才探测 Root，状态查询本身不得触发 su。 */
    @JvmStatic
    fun authorizeRoot(): String {
        val authorized = checkRootAvailable()
        return JSONObject().apply {
            put("success", authorized)
            put("rootStatus", rootAuthorizationStatus)
            put("message", if (authorized) "" else "未取得 Root 授权")
        }.toString()
    }

    /** 在完整与基础产品模式之间切换，并记住用户已经完成选择。 */
    @JvmStatic
    fun setMode(context: Context, mode: String): String {
        if (mode != "full" && mode != "basic") {
            return modeResult(false, currentMode(), "不支持的 Android 运行模式")
        }

        if (mode == "full" && rootAuthorizationStatus != ROOT_STATUS_AUTHORIZED) {
            return modeResult(false, currentMode(), "请先完成 Root 授权")
        }

        currentEngineMode = if (mode == "full") "root" else "foreground"
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .edit()
            .putString(KEY_ENGINE_MODE, currentEngineMode)
            .putBoolean(KEY_MODE_SELECTED, true)
            .apply()
        applicationContext = context.applicationContext
        refreshClipboardListener()
        ensureGestureService(context)
        return modeResult(true, mode, "")
    }

    private fun currentMode(): String {
        return if (currentEngineMode == "root") "full" else "basic"
    }

    private fun modeResult(
        success: Boolean,
        mode: String,
        message: String,
    ): String {
        return JSONObject().apply {
            put("success", success)
            put("mode", mode)
            put("message", message)
        }.toString()
    }

    @JvmStatic
    fun getGestureConfig(context: Context): GestureConfig {
        val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        return GestureConfig(
            enabled = prefs.getBoolean(KEY_GESTURE_ENABLED, false),
            hideOverlay = prefs.getBoolean(KEY_GESTURE_HIDE_OVERLAY, true),
            popupHeightPercent = prefs.getInt(KEY_GESTURE_POPUP_HEIGHT_PERCENT, 64),
            leftWidthDp = prefs.getInt(KEY_GESTURE_LEFT_WIDTH_DP, 109),
            leftHeightDp = prefs.getInt(KEY_GESTURE_LEFT_HEIGHT_DP, 18),
            rightWidthDp = prefs.getInt(KEY_GESTURE_RIGHT_WIDTH_DP, 106),
            rightHeightDp = prefs.getInt(KEY_GESTURE_RIGHT_HEIGHT_DP, 18),
        )
    }

    /** 缓存原生浮窗刚刚调整的高度，供下一次唤起立即读取。 */
    @JvmStatic
    fun rememberGesturePopupHeightPercent(context: Context, heightPercent: Int) {
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .edit()
            .putInt(KEY_GESTURE_POPUP_HEIGHT_PERCENT, heightPercent.coerceIn(30, 90))
            .apply()
    }

    fun isBackgroundKeepAliveEnabled(context: Context): Boolean {
        return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .getBoolean(KEY_BACKGROUND_KEEP_ALIVE, true)
    }

    @JvmStatic
    fun applyAndroidConfig(
        context: Context,
        backgroundKeepAlive: Boolean,
        enabled: Boolean,
        hideOverlay: Boolean,
        popupHeightPercent: Int,
        leftWidthDp: Int,
        leftHeightDp: Int,
        rightWidthDp: Int,
        rightHeightDp: Int,
    ) {
        val previous = getGestureConfig(context)
        val nextLeftWidthDp = leftWidthDp.coerceIn(0, 180)
        val nextLeftHeightDp = leftHeightDp.coerceIn(0, 96)
        val nextRightWidthDp = rightWidthDp.coerceIn(0, 180)
        val nextRightHeightDp = rightHeightDp.coerceIn(0, 96)
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .edit()
            .putBoolean(KEY_BACKGROUND_KEEP_ALIVE, backgroundKeepAlive)
            .putBoolean(KEY_GESTURE_ENABLED, enabled)
            .putBoolean(KEY_GESTURE_HIDE_OVERLAY, hideOverlay)
            .putInt(KEY_GESTURE_POPUP_HEIGHT_PERCENT, popupHeightPercent.coerceIn(30, 90))
            .putInt(KEY_GESTURE_LEFT_WIDTH_DP, nextLeftWidthDp)
            .putInt(KEY_GESTURE_LEFT_HEIGHT_DP, nextLeftHeightDp)
            .putInt(KEY_GESTURE_RIGHT_WIDTH_DP, nextRightWidthDp)
            .putInt(KEY_GESTURE_RIGHT_HEIGHT_DP, nextRightHeightDp)
            .apply()

        if (!enabled || currentEngineMode != "root") {
            stopOverlayService(context)
            return
        }

        val monitorGeometryChanged = previous.enabled != enabled ||
            previous.leftWidthDp != nextLeftWidthDp ||
            previous.leftHeightDp != nextLeftHeightDp ||
            previous.rightWidthDp != nextRightWidthDp ||
            previous.rightHeightDp != nextRightHeightDp
        if (isOverlayServiceRunning(context)) {
            EcoPasteOverlayService.notifyConfigChanged(monitorGeometryChanged)
            return
        }

        val overlayGranted = Build.VERSION.SDK_INT < Build.VERSION_CODES.M ||
            Settings.canDrawOverlays(context)
        if (overlayGranted) {
            startOverlayService(context)
        }
    }

    @JvmStatic
    fun ensureGestureService(context: Context) {
        val config = getGestureConfig(context)
        val overlayGranted = Build.VERSION.SDK_INT < Build.VERSION_CODES.M ||
            Settings.canDrawOverlays(context)
        if (currentEngineMode == "root" && config.enabled && overlayGranted) {
            if (!isOverlayServiceRunning(context)) {
                startOverlayService(context)
            }
        } else {
            stopOverlayService(context)
        }
    }

    /**
     * 注册剪贴板变更监听
     */
    fun addClipboardListener(listener: (String) -> Unit) {
        clipboardChangeListeners.add(listener)
    }

    /** Reconciles the foreground callback and the privileged event-driven root helper. */
    @Synchronized
    private fun refreshClipboardListener() {
        val context = applicationContext ?: return
        val manager = clipboardManager
            ?: (context.getSystemService(Context.CLIPBOARD_SERVICE) as? ClipboardManager).also {
                clipboardManager = it
            }
        val shouldListen = currentEngineMode == "foreground" && foregroundCaptureActive
        if (shouldListen != clipboardListenerRegistered && manager != null) {
            try {
                if (shouldListen) {
                    manager.addPrimaryClipChangedListener(clipChangedListener)
                } else {
                    manager.removePrimaryClipChangedListener(clipChangedListener)
                }
                clipboardListenerRegistered = shouldListen
            } catch (error: Exception) {
                Log.w(TAG, "update clipboard listener failed: ${error.message}")
            }
        }

        if (currentEngineMode == "root") {
            val monitor = rootClipboardMonitor ?: RootClipboardMonitor(context) { text, timestamp, sourcePackage ->
                onClipboardCaptured(text, timestamp, true, sourcePackage)
            }.also {
                rootClipboardMonitor = it
            }
            monitor.start()
        } else {
            rootClipboardMonitor?.stop()
            rootClipboardMonitor = null
        }
    }

    /** Reads and dispatches one native clipboard event without any periodic polling. */
    @JvmStatic
    @Synchronized
    fun captureClipboardChange(
        manager: ClipboardManager?,
        confirmedChange: Boolean,
        sourcePackage: String? = null,
    ) {
        try {
            val description = manager?.primaryClipDescription ?: return
            if (description.extras?.getBoolean(CLIPBOARD_WRITEBACK_MARKER, false) == true) {
                return
            }
            val timestamp = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                description.timestamp
            } else {
                0L
            }
            if (!confirmedChange && timestamp > 0L && timestamp == lastCapturedTimestamp) return

            val clip = manager?.primaryClip ?: return
            if (clip.itemCount <= 0) return
            val context = applicationContext ?: return
            val text = clip.getItemAt(0).coerceToText(context)?.toString() ?: return
            onClipboardCaptured(text, timestamp, confirmedChange, sourcePackage)
        } catch (error: Exception) {
            Log.d(TAG, "read clipboard change failed: ${error.message}")
        }
    }

    /** Writes a marked clip so native capture never turns synchronized content into a local event. */
    @JvmStatic
    fun writeClipboardText(context: Context, text: String) {
        val clip = ClipData.newPlainText("EcoPaste", text)
        clip.description.extras = PersistableBundle().apply {
            putBoolean(CLIPBOARD_WRITEBACK_MARKER, true)
        }
        val manager = context.getSystemService(Context.CLIPBOARD_SERVICE) as? ClipboardManager
            ?: throw IllegalStateException("clipboard service unavailable")
        manager.setPrimaryClip(clip)
    }

    /** Deduplicates multiple native callbacks for the same system clipboard generation. */
    @JvmStatic
    @Synchronized
    fun onClipboardCaptured(
        text: String,
        timestamp: Long,
        confirmedChange: Boolean,
        sourcePackage: String? = null,
    ) {
        if (text.isBlank()) return
        val captureTimestamp = when {
            timestamp > 0L -> timestamp
            confirmedChange -> ++legacyClipboardSequence
            text != lastCapturedText -> ++legacyClipboardSequence
            else -> lastCapturedTimestamp
        }
        if (text == lastCapturedText && captureTimestamp == lastCapturedTimestamp) return
        val source = resolveSourceApp(sourcePackage)
        try {
            if (!captureClipboardText(text, source.packageName, source.appName, source.iconPng)) return
        } catch (error: Throwable) {
            Log.w(TAG, "send clipboard capture to Rust failed: ${error.message}")
            return
        }
        lastCapturedText = text
        lastCapturedTimestamp = captureTimestamp
        for (listener in clipboardChangeListeners) {
            try {
                listener(text)
            } catch (_: Exception) {}
        }
    }

    private data class SourceAppMetadata(
        val packageName: String,
        val appName: String,
        val iconPng: ByteArray,
    )

    /** Resolves only an explicitly observed package and falls back to an unknown source. */
    private fun resolveSourceApp(packageName: String?): SourceAppMetadata {
        val context = applicationContext
        if (context == null || packageName.isNullOrBlank()) {
            return SourceAppMetadata("", "", byteArrayOf())
        }
        sourceAppCache[packageName]?.let { return it }
        return try {
            val packageManager = context.packageManager
            val applicationInfo = packageManager.getApplicationInfo(packageName, 0)
            val appName = packageManager.getApplicationLabel(applicationInfo).toString()
            val drawable = packageManager.getApplicationIcon(applicationInfo)
            val bitmap = Bitmap.createBitmap(128, 128, Bitmap.Config.ARGB_8888)
            val canvas = Canvas(bitmap)
            drawable.setBounds(0, 0, bitmap.width, bitmap.height)
            drawable.draw(canvas)
            val iconPng = ByteArrayOutputStream().use { stream ->
                bitmap.compress(Bitmap.CompressFormat.PNG, 100, stream)
                stream.toByteArray()
            }
            bitmap.recycle()
            SourceAppMetadata(packageName, appName.ifBlank { packageName }, iconPng).also {
                sourceAppCache.put(packageName, it)
            }
        } catch (error: Exception) {
            Log.d(TAG, "resolve clipboard source app failed for $packageName: ${error.message}")
            SourceAppMetadata(packageName, packageName, byteArrayOf())
        }
    }

    /**
     * 检测设备是否有 Root 权限
     */
    @JvmStatic
    fun checkRootAvailable(): Boolean {
        val available = try {
            val process = ProcessBuilder("su", "-c", "id")
                .redirectErrorStream(true)
                .start()
            if (!waitForProcess(process, 3_000L)) {
                false
            } else {
                process.inputStream.bufferedReader().use { reader ->
                    process.exitValue() == 0 && reader.readText().contains("uid=0")
                }
            }
        } catch (_: Exception) {
            false
        }
        rootAuthorizationStatus = if (available) {
            ROOT_STATUS_AUTHORIZED
        } else {
            ROOT_STATUS_UNAVAILABLE
        }
        return available
    }

    /** Waits for short root commands without depending on newer Android Process APIs. */
    private fun waitForProcess(process: Process, timeoutMs: Long): Boolean {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        while (System.nanoTime() < deadline) {
            try {
                process.exitValue()
                return true
            } catch (_: IllegalThreadStateException) {
                try {
                    Thread.sleep(50L)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    process.destroy()
                    return false
                }
            }
        }
        process.destroy()
        return false
    }
}
