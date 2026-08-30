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
import android.net.ConnectivityManager
import android.net.LinkProperties
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.net.Uri
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.PowerManager
import android.os.PersistableBundle
import android.provider.Settings
import android.util.Log
import android.webkit.MimeTypeMap
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import org.json.JSONObject
import java.io.File
import java.io.FileInputStream
import java.lang.ref.WeakReference
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

object EcoPasteBridge {
    private const val TAG = "EcoPasteBridge"
    private const val FILE_ACTION_TAG = "EcoPasteFileAction"
    private const val PREFS_NAME = "ecopaste_android"
    private const val KEY_ENGINE_MODE = "engine_mode"
    private const val KEY_GESTURE_ENABLED = "gesture_enabled"
    private const val KEY_GESTURE_HIDE_OVERLAY = "gesture_hide_overlay"
    private const val KEY_GESTURE_POPUP_HEIGHT_PERCENT = "gesture_popup_height_percent"
    private const val KEY_GESTURE_LEFT_WIDTH_DP = "gesture_left_width_dp"
    private const val KEY_GESTURE_LEFT_HEIGHT_DP = "gesture_left_height_dp"
    private const val KEY_GESTURE_RIGHT_WIDTH_DP = "gesture_right_width_dp"
    private const val KEY_GESTURE_RIGHT_HEIGHT_DP = "gesture_right_height_dp"
    private const val CLIPBOARD_WRITEBACK_MARKER = "com.ayangweb.eco_paste.WRITEBACK"
    private var currentActivityRef: WeakReference<Activity>? = null
    private var lanDiscoveryLock: WifiManager.MulticastLock? = null
    private var lanDiscoveryLeaseCount = 0
    private var applicationContext: Context? = null
    private var clipboardManager: ClipboardManager? = null
    private var clipboardListenerRegistered = false
    private var syncNetworkCallback: ConnectivityManager.NetworkCallback? = null
    private var syncNetworkFingerprint: String? = null
    private var rootClipboardMonitor: RootClipboardMonitor? = null
    private var foregroundCaptureActive = false
    var currentEngineMode: String = "accessibility" // "accessibility", "root", "foreground"
        private set

    data class GestureConfig(
        val enabled: Boolean,
        val hideOverlay: Boolean,
        val popupHeightPercent: Int,
        val leftWidthDp: Int,
        val leftHeightDp: Int,
        val rightWidthDp: Int,
        val rightHeightDp: Int,
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
    external fun captureClipboardText(text: String): Boolean

    @JvmStatic
    external fun pasteOverlayItem(id: String): Boolean

    @JvmStatic
    external fun persistOverlayPanelHeightPercent(heightPercent: Int): Boolean

    @JvmStatic
    external fun notifySyncNetworkChanged()

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

    private fun mimeType(file: File): String {
        val extension = file.extension.lowercase()
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension)
            ?: "application/octet-stream"
    }

    private fun fileActionResult(status: String, message: String = ""): String {
        return JSONObject().apply {
            put("status", status)
            put("message", message)
        }.toString()
    }

    @JvmStatic
    fun initialize(context: Context) {
        applicationContext = context.applicationContext
        currentEngineMode = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .getString(KEY_ENGINE_MODE, "accessibility")
            ?.takeIf { it == "accessibility" || it == "root" || it == "foreground" }
            ?: "accessibility"
        refreshClipboardListener()
        registerSyncNetworkCallback(context.applicationContext)
    }

    /** Wakes native synchronization only when the active Wi-Fi route appears or changes. */
    @Synchronized
    private fun registerSyncNetworkCallback(context: Context) {
        if (syncNetworkCallback != null) return

        val connectivityManager = context.getSystemService(Context.CONNECTIVITY_SERVICE)
            as? ConnectivityManager ?: return
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                connectivityManager.getLinkProperties(network)?.let { properties ->
                    notifySyncRouteChanged(network, properties)
                }
            }

            override fun onLinkPropertiesChanged(network: Network, properties: LinkProperties) {
                notifySyncRouteChanged(network, properties)
            }

            override fun onLost(network: Network) {
                synchronized(this@EcoPasteBridge) {
                    if (syncNetworkFingerprint?.startsWith("$network:") == true) {
                        syncNetworkFingerprint = null
                    }
                }
            }
        }
        val request = NetworkRequest.Builder()
            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
            .build()
        try {
            connectivityManager.registerNetworkCallback(request, callback)
            syncNetworkCallback = callback
        } catch (error: RuntimeException) {
            Log.w(TAG, "register sync network callback failed: ${error.message}")
        }
    }

    /** Coalesces the paired callbacks Android emits for the same Wi-Fi route. */
    private fun notifySyncRouteChanged(network: Network, properties: LinkProperties) {
        val addresses = properties.linkAddresses
            .map { it.address.hostAddress.orEmpty() }
            .filter { it.isNotEmpty() }
            .sorted()
        if (addresses.isEmpty()) return

        val fingerprint = buildString {
            append(network)
            append(':')
            append(addresses.joinToString(","))
        }
        synchronized(this) {
            if (syncNetworkFingerprint == fingerprint) return
            syncNetworkFingerprint = fingerprint
        }
        notifySyncNetworkChangedSafely("route:$fingerprint")
    }

    /** Keeps callbacks safe when an Android service starts before the Tauri runtime is ready. */
    private fun notifySyncNetworkChangedSafely(reason: String) {
        try {
            notifySyncNetworkChanged()
        } catch (error: Throwable) {
            Log.w(TAG, "notify sync network change failed ($reason): ${error.message}")
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
        val overlayGranted = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Settings.canDrawOverlays(context)
        } else {
            true
        }

        val accessibilityGranted = isAccessibilityServiceEnabled(context)

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

        val rootAvailable = checkRootAvailable()
        val rootClipboardGranted = rootAvailable && isRootClipboardGranted(context)
        val overlayRunning = isOverlayServiceRunning(context)

        val json = JSONObject().apply {
            put("overlayGranted", overlayGranted)
            put("accessibilityGranted", accessibilityGranted)
            put("notificationGranted", notificationGranted)
            put("batteryIgnored", batteryIgnored)
            put("rootAvailable", rootAvailable)
            put("rootClipboardGranted", rootClipboardGranted)
            put("overlayServiceRunning", overlayRunning)
            put("engineMode", currentEngineMode)
        }
        return json.toString()
    }

    /**
     * 判断无障碍服务是否已开启
     */
    @JvmStatic
    fun isAccessibilityServiceEnabled(context: Context): Boolean {
        val expectedComponentName = "${context.packageName}/${EcoPasteAccessibilityService::class.java.canonicalName}"
        val enabledServices = Settings.Secure.getString(
            context.contentResolver,
            Settings.Secure.ENABLED_ACCESSIBILITY_SERVICES
        ) ?: return false

        return enabledServices.split(":").any {
            it.equals(expectedComponentName, ignoreCase = true) ||
            it.contains(EcoPasteAccessibilityService::class.java.simpleName, ignoreCase = true)
        }
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
                    "accessibility" -> requestAccessibilityPermission(targetContext)
                    "battery" -> requestBatteryOptimization(targetContext)
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
     * 请求打开系统无障碍设置页
     */
    @JvmStatic
    fun requestAccessibilityPermission(context: Context) {
        try {
            val intent = Intent(Settings.ACTION_ACCESSIBILITY_SETTINGS).apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            context.startActivity(intent)
        } catch (e: Exception) {
            openAppDetailsSettings(context)
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

    /**
     * 开启/停止悬浮手势服务
     */
    @JvmStatic
    fun setOverlayServiceEnabled(context: Context, enable: Boolean) {
        if (enable) {
            startOverlayService(context)
        } else {
            stopOverlayService(context)
        }
    }

    /** 仅在 Root 可用且用户已启用时启动全局输入监控服务。 */
    @JvmStatic
    fun startOverlayService(context: Context) {
        if (!getGestureConfig(context).enabled || !checkRootAvailable()) {
            Log.w(TAG, "gesture service requires enabled Root environment")
            stopLegacyOverlayService(context)
            return
        }
        startLegacyOverlayService(context)
    }

    /** 停止 Root 手势监控服务。 */
    @JvmStatic
    fun stopOverlayService(context: Context) {
        stopLegacyOverlayService(context)
    }

    private fun startLegacyOverlayService(context: Context) {
        val intent = Intent(context, EcoPasteOverlayService::class.java)
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        } catch (e: Exception) {
            Log.e(TAG, "start overlay service failed: ${e.message}")
        }
    }

    private fun stopLegacyOverlayService(context: Context) {
        val intent = Intent(context, EcoPasteOverlayService::class.java)
        try {
            context.stopService(intent)
        } catch (e: Exception) {
            Log.e(TAG, "stop overlay service failed: ${e.message}")
        }
    }

    /** 判断 Root 手势监控前台服务是否正在运行。 */
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
        if (EcoPasteAccessibilityService.instance?.performPaste() == true) {
            return true
        }

        if (currentEngineMode != "root" || !checkRootAvailable()) return false

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

    /**
     * 设置引擎模式
     */
    @JvmStatic
    fun setEngine(context: Context, mode: String): String {
        if (mode != "accessibility" && mode != "root" && mode != "foreground") {
            return engineResult(false, currentEngineMode, false, "不支持的剪贴板引擎")
        }

        if (mode == "root") {
            if (!checkRootAvailable()) {
                return engineResult(false, currentEngineMode, false, "未检测到 Root 权限")
            }
            if (!grantClipboardAccessViaRoot(context) || !isRootClipboardGranted(context)) {
                return engineResult(false, currentEngineMode, false, "Root AppOps 授权失败")
            }
        }

        currentEngineMode = mode
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .edit()
            .putString(KEY_ENGINE_MODE, mode)
            .apply()
        applicationContext = context.applicationContext
        refreshClipboardListener()
        ensureGestureService(context)
        return engineResult(true, mode, mode != "root" || isRootClipboardGranted(context), "")
    }

    private fun engineResult(
        success: Boolean,
        mode: String,
        rootClipboardGranted: Boolean,
        message: String,
    ): String {
        return JSONObject().apply {
            put("success", success)
            put("mode", mode)
            put("rootClipboardGranted", rootClipboardGranted)
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

    @JvmStatic
    fun applyGestureConfig(
        context: Context,
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
            .putBoolean(KEY_GESTURE_ENABLED, enabled)
            .putBoolean(KEY_GESTURE_HIDE_OVERLAY, hideOverlay)
            .putInt(KEY_GESTURE_POPUP_HEIGHT_PERCENT, popupHeightPercent.coerceIn(30, 90))
            .putInt(KEY_GESTURE_LEFT_WIDTH_DP, nextLeftWidthDp)
            .putInt(KEY_GESTURE_LEFT_HEIGHT_DP, nextLeftHeightDp)
            .putInt(KEY_GESTURE_RIGHT_WIDTH_DP, nextRightWidthDp)
            .putInt(KEY_GESTURE_RIGHT_HEIGHT_DP, nextRightHeightDp)
            .apply()

        if (!enabled) {
            stopLegacyOverlayService(context)
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

        if (checkRootAvailable()) {
            startLegacyOverlayService(context)
        }
    }

    @JvmStatic
    fun ensureGestureService(context: Context) {
        val config = getGestureConfig(context)
        if (currentEngineMode == "root" || (config.enabled && checkRootAvailable())) {
            startLegacyOverlayService(context)
        } else {
            stopLegacyOverlayService(context)
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
            val monitor = rootClipboardMonitor ?: RootClipboardMonitor(context) { text, timestamp ->
                onClipboardCaptured(text, timestamp, true)
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
    fun captureClipboardChange(manager: ClipboardManager?, confirmedChange: Boolean) {
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
            val context = applicationContext ?: EcoPasteAccessibilityService.instance ?: return
            val text = clip.getItemAt(0).coerceToText(context)?.toString() ?: return
            onClipboardCaptured(text, timestamp, confirmedChange)
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
    fun onClipboardCaptured(text: String, timestamp: Long, confirmedChange: Boolean) {
        if (text.isBlank()) return
        val captureTimestamp = when {
            timestamp > 0L -> timestamp
            confirmedChange -> ++legacyClipboardSequence
            text != lastCapturedText -> ++legacyClipboardSequence
            else -> lastCapturedTimestamp
        }
        if (text == lastCapturedText && captureTimestamp == lastCapturedTimestamp) return
        try {
            if (!captureClipboardText(text)) return
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

    /**
     * 检测设备是否有 Root 权限
     */
    @JvmStatic
    fun checkRootAvailable(): Boolean {
        return try {
            val process = ProcessBuilder("su", "-c", "id")
                .redirectErrorStream(true)
                .start()
            if (!waitForProcess(process, 3_000L)) return false
            process.inputStream.bufferedReader().use { reader ->
                process.exitValue() == 0 && reader.readText().contains("uid=0")
            }
        } catch (_: Exception) {
            false
        }
    }

    /**
     * 尝试通过 Root 命令授权 appops READ_CLIPBOARD
     */
    @JvmStatic
    fun grantClipboardAccessViaRoot(context: Context): Boolean {
        return try {
            val pkg = context.packageName
            val cmd = "cmd appops set $pkg READ_CLIPBOARD allow"
            val process = ProcessBuilder("su", "-c", cmd)
                .redirectErrorStream(true)
                .start()
            if (!waitForProcess(process, 5_000L)) return false
            process.inputStream.bufferedReader().use { it.readText() }
            process.exitValue() == 0
        } catch (error: Exception) {
            Log.e(TAG, "grant root clipboard AppOps failed: ${error.message}", error)
            false
        }
    }

    @JvmStatic
    fun isRootClipboardGranted(context: Context): Boolean {
        return try {
            val pkg = context.packageName
            val process = ProcessBuilder("su", "-c", "cmd appops get $pkg READ_CLIPBOARD")
                .redirectErrorStream(true)
                .start()
            if (!waitForProcess(process, 3_000L)) return false
            val output = process.inputStream.bufferedReader().use { it.readText() }
            process.exitValue() == 0 && Regex("READ_CLIPBOARD:\\s*allow").containsMatchIn(output)
        } catch (error: Exception) {
            Log.d(TAG, "read root clipboard AppOps failed: ${error.message}")
            false
        }
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
