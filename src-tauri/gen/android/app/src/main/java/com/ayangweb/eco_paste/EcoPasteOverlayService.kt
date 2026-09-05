package com.ayangweb.eco_paste

import android.app.KeyguardManager
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.res.Configuration
import android.graphics.PixelFormat
import android.graphics.Point
import android.graphics.Rect
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import android.os.SystemClock
import android.provider.Settings
import android.util.Log
import android.view.Gravity
import android.view.View
import android.view.WindowManager
import androidx.core.app.NotificationCompat

/** Root-only 全局上滑监控；左右感应区窗口用于阻止点击穿透。 */
class EcoPasteOverlayService : Service() {

    companion object {
        private const val TAG = "EcoPasteOverlayService"
        private const val CHANNEL_ID = "ecopaste_overlay_channel"
        private const val NOTIFICATION_ID = 1002
        private const val SWIPE_MAX_DURATION_MS = 1_200L
        private const val SWIPE_MOVE_THRESHOLD_PX = 8f
        private const val BACK_EDGE_WIDTH_DP = 28
        private const val BACK_MOVE_THRESHOLD_DP = 52
        private const val SYSTEM_GESTURE_HEIGHT_DP = 48
        private const val SUMMON_DEBOUNCE_MS = 240L
        private val MONITOR_RETRY_DELAYS_MS = longArrayOf(
            2_000L,
            5_000L,
            15_000L,
            30_000L,
            60_000L,
            300_000L,
        )
        private var instance: EcoPasteOverlayService? = null

        fun notifyConfigChanged(monitorGeometryChanged: Boolean) {
            instance?.mainHandler?.post {
                instance?.monitorFailureCount = 0
                instance?.reconcileState(
                    refreshRoot = false,
                    forceMonitorRestart = monitorGeometryChanged,
                )
            }
        }

        fun isGestureMonitorReady(): Boolean {
            return instance?.gestureMonitorReady == true
        }
    }

    private val mainHandler = Handler(Looper.getMainLooper())
    private val rootCheckExecutor = reclaimingFixedThreadPool(1)
    private var windowManager: WindowManager? = null
    private var leftIndicator: View? = null
    private var rightIndicator: View? = null
    private var overlayPanel: EcoPasteOverlayPanel? = null
    private var rootInputMonitor: RootInputMonitor? = null
    private var gestureMonitorReady = false
    private var activeMonitorSignature: String? = null
    private var rootAvailable = false
    private var panelSessionId: Long? = null
    private var lastSummonAt = 0L
    private var rootCheckGeneration = 0
    private var monitorFailureCount = 0
    private val monitorRestartRunnable = Runnable {
        if (rootInputMonitor == null) {
            reconcileState(
                refreshRoot = true,
                forceMonitorRestart = false,
                resetFailures = false,
            )
        }
    }

    private val screenStateReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            when (intent?.action) {
                Intent.ACTION_SCREEN_OFF -> suspendGesture()
                Intent.ACTION_SCREEN_ON,
                Intent.ACTION_USER_PRESENT,
                Intent.ACTION_USER_UNLOCKED -> {
                    monitorFailureCount = 0
                    reconcileState(refreshRoot = true, forceMonitorRestart = false)
                }
            }
        }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        EcoPasteBridge.initialize(applicationContext)
        instance = this
        windowManager = getSystemService(Context.WINDOW_SERVICE) as? WindowManager
        overlayPanel = windowManager?.let { manager ->
            EcoPasteOverlayPanel(this, manager) { sessionId ->
                panelSessionId = sessionId
                rootInputMonitor?.setPanelSession(sessionId)
            }
        }
        startForegroundNotification()
        registerScreenStateReceiver()
        reconcileState(refreshRoot = true, forceMonitorRestart = false)
        Log.i(TAG, "Root gesture service created")
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        monitorFailureCount = 0
        reconcileState(refreshRoot = true, forceMonitorRestart = false)
        return START_STICKY
    }

    override fun onConfigurationChanged(newConfig: Configuration) {
        super.onConfigurationChanged(newConfig)
        activeMonitorSignature = null
        overlayPanel?.hide()
        removeIndicators()
        reconcileState(refreshRoot = false, forceMonitorRestart = true)
    }

    private fun registerScreenStateReceiver() {
        val filter = IntentFilter().apply {
            addAction(Intent.ACTION_SCREEN_OFF)
            addAction(Intent.ACTION_SCREEN_ON)
            addAction(Intent.ACTION_USER_PRESENT)
            addAction(Intent.ACTION_USER_UNLOCKED)
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            registerReceiver(screenStateReceiver, filter, Context.RECEIVER_NOT_EXPORTED)
        } else {
            @Suppress("DEPRECATION")
            registerReceiver(screenStateReceiver, filter)
        }
    }

    private fun startForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                getString(R.string.overlay_service_channel_name),
                NotificationManager.IMPORTANCE_LOW,
            ).apply {
                description = getString(R.string.overlay_service_channel_desc)
                setShowBadge(false)
                lockscreenVisibility = Notification.VISIBILITY_SECRET
            }
            val manager = getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager
            manager?.createNotificationChannel(channel)
        }

        val pendingIntent = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        val notification = NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle(getString(R.string.overlay_service_notification_title))
            .setContentText(getString(R.string.overlay_service_notification_text))
            .setSmallIcon(R.mipmap.ic_launcher)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setVisibility(NotificationCompat.VISIBILITY_SECRET)
            .build()
        startForeground(NOTIFICATION_ID, notification)
    }

    /** 统一按开关、Root、亮屏和解锁状态决定是否运行。 */
    private fun reconcileState(
        refreshRoot: Boolean,
        forceMonitorRestart: Boolean,
        resetFailures: Boolean = true,
    ) {
        if (resetFailures) monitorFailureCount = 0
        val config = EcoPasteBridge.getGestureConfig(this)
        if (refreshRoot) {
            val generation = ++rootCheckGeneration
            rootCheckExecutor.execute {
                val available = EcoPasteBridge.checkRootAvailable()
                mainHandler.post {
                    if (generation != rootCheckGeneration) return@post
                    rootAvailable = available
                    reconcileState(
                        refreshRoot = false,
                        forceMonitorRestart = forceMonitorRestart,
                        resetFailures = false,
                    )
                }
            }
            return
        }

        if (!config.enabled || !rootAvailable || !isInteractiveAndUnlocked()) {
            suspendGesture()
            return
        }

        if (forceMonitorRestart) {
            stopRootInputMonitor()
        }
        startRootInputMonitor(config)
        updateIndicatorWindows()
    }

    private fun isInteractiveAndUnlocked(): Boolean {
        val powerManager = getSystemService(Context.POWER_SERVICE) as? PowerManager
        val keyguardManager = getSystemService(Context.KEYGUARD_SERVICE) as? KeyguardManager
        return powerManager?.isInteractive == true && keyguardManager?.isDeviceLocked != true
    }

    private fun suspendGesture() {
        rootCheckGeneration += 1
        overlayPanel?.hide()
        stopRootInputMonitor()
        removeIndicators()
    }

    private fun startRootInputMonitor(config: EcoPasteBridge.GestureConfig) {
        val bounds = displayBounds()
        val density = resources.displayMetrics.density
        val leftWidth = dpToPx(config.leftWidthDp, density).coerceAtMost(bounds.width() / 2)
        val leftHeight = dpToPx(config.leftHeightDp, density).coerceAtMost(bounds.height())
        val rightWidth = dpToPx(config.rightWidthDp, density).coerceAtMost(bounds.width() / 2)
        val rightHeight = dpToPx(config.rightHeightDp, density).coerceAtMost(bounds.height())
        val panelTop = bounds.height() -
            (bounds.height() * config.popupHeightPercent.coerceIn(30, 90) / 100f).toInt()
        val backEdgeWidth = dpToPx(BACK_EDGE_WIDTH_DP, density)
        val backThreshold = dpToPx(BACK_MOVE_THRESHOLD_DP, density).toFloat()
        val systemGestureHeight = dpToPx(SYSTEM_GESTURE_HEIGHT_DP, density)
        val signature = listOf(
            bounds.width(),
            bounds.height(),
            leftWidth,
            leftHeight,
            rightWidth,
            rightHeight,
            panelTop,
            systemGestureHeight,
        ).joinToString(":")
        if (activeMonitorSignature == signature && rootInputMonitor != null) return

        stopRootInputMonitor()
        lateinit var monitor: RootInputMonitor
        monitor = RootInputMonitor(
            context = this,
            onReady = { ready ->
                mainHandler.post {
                    if (ready && rootInputMonitor === monitor) {
                        gestureMonitorReady = true
                        monitorFailureCount = 0
                        mainHandler.removeCallbacks(monitorRestartRunnable)
                        Log.i(TAG, "Root InputMonitor ready: $signature")
                    } else if (rootInputMonitor === monitor) {
                        gestureMonitorReady = false
                        rootInputMonitor = null
                        activeMonitorSignature = null
                        removeIndicators()
                        mainHandler.removeCallbacks(monitorRestartRunnable)
                        val retryDelay = MONITOR_RETRY_DELAYS_MS.getOrNull(monitorFailureCount)
                        if (retryDelay == null) {
                            Log.w(
                                TAG,
                                "Root InputMonitor retry suspended until the next resume event",
                            )
                        } else {
                            monitorFailureCount += 1
                            mainHandler.postDelayed(monitorRestartRunnable, retryDelay)
                            Log.w(
                                TAG,
                                "Root InputMonitor exited; restart scheduled in ${retryDelay}ms",
                            )
                        }
                    }
                }
            },
            onSwipe = {
                mainHandler.post { showClipboardPanel() }
            },
            onBackSwipe = { sessionId ->
                mainHandler.post { navigateBackOrHideClipboardPanel(sessionId) }
            },
            onHomeSwipe = { sessionId ->
                mainHandler.post { hideClipboardPanel(sessionId) }
            },
        )
        rootInputMonitor = monitor
        activeMonitorSignature = signature
        monitor.setPanelSession(panelSessionId)
        monitor.start(
            displayId = 0,
            displayWidth = bounds.width(),
            displayHeight = bounds.height(),
            leftSensorWidth = leftWidth,
            leftSensorHeight = leftHeight,
            rightSensorWidth = rightWidth,
            rightSensorHeight = rightHeight,
            systemGestureHeight = systemGestureHeight,
            swipeThreshold = SWIPE_MOVE_THRESHOLD_PX,
            maxDurationMs = SWIPE_MAX_DURATION_MS,
            panelTop = panelTop,
            backEdgeWidth = backEdgeWidth,
            backThreshold = backThreshold,
        )
    }

    private fun stopRootInputMonitor() {
        mainHandler.removeCallbacks(monitorRestartRunnable)
        gestureMonitorReady = false
        rootInputMonitor?.stop()
        rootInputMonitor = null
        activeMonitorSignature = null
    }

    private fun updateIndicatorWindows() {
        val config = EcoPasteBridge.getGestureConfig(this)
        val shouldCreate = config.enabled &&
            rootAvailable &&
            isInteractiveAndUnlocked() &&
            (Build.VERSION.SDK_INT < Build.VERSION_CODES.M || Settings.canDrawOverlays(this))

        removeIndicators()
        if (!shouldCreate) return

        val wm = windowManager ?: return
        val density = resources.displayMetrics.density
        val layoutType = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
        } else {
            @Suppress("DEPRECATION")
            WindowManager.LayoutParams.TYPE_PHONE
        }
        val flags = WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE or
            WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or
            WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS or
            WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN

        leftIndicator = createIndicator("↖ 上滑呼出", density, !config.hideOverlay)
        rightIndicator = createIndicator("上滑呼出 ↗", density, !config.hideOverlay)
        try {
            val leftWidth = dpToPx(config.leftWidthDp, density)
            val leftHeight = dpToPx(config.leftHeightDp, density)
            if (leftWidth > 0 && leftHeight > 0) {
                wm.addView(
                    leftIndicator,
                    createLayoutParams(
                        leftWidth,
                        leftHeight,
                        layoutType,
                        flags,
                        Gravity.BOTTOM or Gravity.START,
                    ),
                )
            } else {
                leftIndicator = null
            }

            val rightWidth = dpToPx(config.rightWidthDp, density)
            val rightHeight = dpToPx(config.rightHeightDp, density)
            if (rightWidth > 0 && rightHeight > 0) {
                wm.addView(
                    rightIndicator,
                    createLayoutParams(
                        rightWidth,
                        rightHeight,
                        layoutType,
                        flags,
                        Gravity.BOTTOM or Gravity.END,
                    ),
                )
            } else {
                rightIndicator = null
            }
        } catch (error: Exception) {
            Log.e(TAG, "failed to add gesture indicators: ${error.message}", error)
            removeIndicators()
        }
    }

    private fun createLayoutParams(
        width: Int,
        height: Int,
        type: Int,
        flags: Int,
        gravityValue: Int,
    ): WindowManager.LayoutParams {
        return WindowManager.LayoutParams(width, height, type, flags, PixelFormat.TRANSLUCENT).apply {
            gravity = gravityValue
            x = 0
            y = 0
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                fitInsetsTypes = 0
                fitInsetsSides = 0
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                layoutInDisplayCutoutMode =
                    WindowManager.LayoutParams.LAYOUT_IN_DISPLAY_CUTOUT_MODE_SHORT_EDGES
            }
        }
    }

    private fun createIndicator(label: String, density: Float, showVisual: Boolean): View {
        return android.widget.TextView(this).apply {
            text = if (showVisual) label else ""
            textSize = 10.5f
            setTextColor(android.graphics.Color.WHITE)
            gravity = Gravity.CENTER
            background = if (showVisual) {
                android.graphics.drawable.GradientDrawable().apply {
                    shape = android.graphics.drawable.GradientDrawable.RECTANGLE
                    setColor(android.graphics.Color.parseColor("#B3007AFF"))
                    cornerRadii = floatArrayOf(
                        16 * density,
                        16 * density,
                        16 * density,
                        16 * density,
                        0f,
                        0f,
                        0f,
                        0f,
                    )
                    setStroke(
                        (1.5f * density).toInt(),
                        android.graphics.Color.parseColor("#E6FFFFFF"),
                    )
                }
            } else {
                android.graphics.drawable.ColorDrawable(android.graphics.Color.TRANSPARENT)
            }
            setOnTouchListener { _, _ -> true }
        }
    }

    private fun displayBounds(): Rect {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            return windowManager?.currentWindowMetrics?.bounds
                ?: Rect(0, 0, resources.displayMetrics.widthPixels, resources.displayMetrics.heightPixels)
        }
        val size = Point()
        @Suppress("DEPRECATION")
        windowManager?.defaultDisplay?.getRealSize(size)
        return Rect(0, 0, size.x, size.y)
    }

    private fun dpToPx(value: Int, density: Float): Int {
        return (value * density).toInt().coerceAtLeast(0)
    }

    private fun showClipboardPanel() {
        val config = EcoPasteBridge.getGestureConfig(this)
        if (!config.enabled || !rootAvailable || !isInteractiveAndUnlocked()) {
            Log.i(TAG, "ignore swipe while gesture runtime is suspended")
            return
        }
        val now = SystemClock.uptimeMillis()
        if (now - lastSummonAt < SUMMON_DEBOUNCE_MS) return
        lastSummonAt = now
        overlayPanel?.show(config.popupHeightPercent)
    }

    /** 只允许当前会话的返回或回桌面事件收起面板，丢弃延迟到达的旧事件。 */
    private fun hideClipboardPanel(sessionId: Long?) {
        if (sessionId != null && sessionId != panelSessionId) return
        overlayPanel?.hide(sessionId)
    }

    /** 优先关闭面板内的详情层；仅在根层才收起整个悬浮面板。 */
    private fun navigateBackOrHideClipboardPanel(sessionId: Long?) {
        if (sessionId != null && sessionId != panelSessionId) return
        if (overlayPanel?.navigateBack(sessionId) == true) return

        hideClipboardPanel(sessionId)
    }

    private fun removeIndicators() {
        try {
            leftIndicator?.let { windowManager?.removeView(it) }
            rightIndicator?.let { windowManager?.removeView(it) }
        } catch (error: Exception) {
            Log.w(TAG, "remove gesture indicators failed: ${error.message}")
        }
        leftIndicator = null
        rightIndicator = null
    }

    override fun onDestroy() {
        try {
            unregisterReceiver(screenStateReceiver)
        } catch (_: Exception) {}
        suspendGesture()
        overlayPanel?.destroy()
        overlayPanel = null
        rootCheckExecutor.shutdownNow()
        instance = null
        super.onDestroy()
        Log.i(TAG, "Root gesture service destroyed")
    }
}
