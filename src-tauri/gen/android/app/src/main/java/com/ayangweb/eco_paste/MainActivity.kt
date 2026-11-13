package com.ayangweb.eco_paste

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.webkit.WebView
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.splashscreen.SplashScreen.Companion.installSplashScreen
import androidx.core.splashscreen.SplashScreenViewProvider

class MainActivity : TauriActivity() {
    override val handleBackNavigation: Boolean = false

    private var startupContentReady = false
    private var startupSplash: SplashScreenViewProvider? = null
    private var startupWebView: WebView? = null
    private var createDocumentCallback: ((Uri?) -> Unit)? = null
    private var openDocumentTreeCallback: ((Uri?) -> Unit)? = null
    private val createDocumentLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult(),
    ) { result ->
        val callback = createDocumentCallback
        createDocumentCallback = null
        val targetUri = result.data?.data.takeIf { result.resultCode == RESULT_OK }
        EcoPasteBridge.logFileAction(
            "native.save.result",
            "resultCode=${result.resultCode} uri=$targetUri callback=${callback != null}",
        )
        callback?.invoke(targetUri)
    }
    private val openDocumentTreeLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult(),
    ) { result ->
        val callback = openDocumentTreeCallback
        openDocumentTreeCallback = null
        val targetUri = result.data?.data.takeIf { result.resultCode == RESULT_OK }
        EcoPasteBridge.logFileAction(
            "native.save.tree.result",
            "resultCode=${result.resultCode} uri=$targetUri callback=${callback != null}",
        )
        callback?.invoke(targetUri)
    }

    private external fun initNdkContext(context: Context)

    override fun onCreate(savedInstanceState: Bundle?) {
        val createStartedAt = android.os.SystemClock.elapsedRealtime()
        val createWallTime = System.currentTimeMillis()
        android.util.Log.i(
            "EcoPasteStartup",
            "startup timing: stage=android-activity-begin atMs=$createWallTime",
        )
        val splashScreen = installSplashScreen()
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        splashScreen.setOnExitAnimationListener { splash ->
            startupSplash = splash
            dismissStartupSplashIfReady()
        }
        android.util.Log.i(
            "EcoPasteStartup",
            "startup timing: stage=android-super-ready atMs=${System.currentTimeMillis()} elapsedMs=${android.os.SystemClock.elapsedRealtime() - createStartedAt}",
        )
        try {
            initNdkContext(applicationContext)
            EcoPasteBridge.initNdkContext(applicationContext)
            EcoPasteBridge.refreshAutomaticDeviceName(
                EcoPasteBridge.getDeviceName(applicationContext),
            )
            EcoPasteBridge.logFileAction("native.init", "JNI context initialized")
        } catch (e: Throwable) {
            android.util.Log.w("MainActivity", "initNdkContext warning: ${e.message}")
        }
        EcoPasteBridge.setCurrentActivity(this)
        EcoPasteBridge.initialize(applicationContext)
        android.util.Log.i(
            "EcoPasteStartup",
            "startup timing: stage=android-bridge-ready atMs=${System.currentTimeMillis()} elapsedMs=${android.os.SystemClock.elapsedRealtime() - createStartedAt}",
        )
        if (savedInstanceState == null) {
            EcoPasteShareReceiver.receive(this, intent)
        }
        android.util.Log.i(
            "EcoPasteStartup",
            "startup timing: stage=android-activity startedAtMs=$createWallTime createMs=${android.os.SystemClock.elapsedRealtime() - createStartedAt}",
        )
    }

    /** WebView 尚未绘制网页时沿用原生窗口背景，避免短暂露出默认白色。 */
    override fun onWebViewCreate(webView: WebView) {
        super.onWebViewCreate(webView)
        startupWebView = webView
        webView.setBackgroundColor(resources.getColor(R.color.app_background, theme))
        android.util.Log.i(
            "EcoPasteStartup",
            "startup timing: stage=android-webview-created atMs=${System.currentTimeMillis()}",
        )
    }

    /** 页面外壳提交后，等待对应网页内容可绘制再撤掉原生 Logo。 */
    fun onStartupContentReady() {
        val webView = startupWebView ?: return
        webView.postVisualStateCallback(0L, object : WebView.VisualStateCallback() {
            override fun onComplete(requestId: Long) {
                startupContentReady = true
                webView.invalidate()
                android.util.Log.i(
                    "EcoPasteStartup",
                    "startup timing: stage=android-content-drawable atMs=${System.currentTimeMillis()}",
                )
                dismissStartupSplashIfReady()
            }
        })
    }

    /** Logo 覆盖初始化过程，同时允许下方 WebView 正常绘制。 */
    private fun dismissStartupSplashIfReady() {
        if (!startupContentReady) return
        val splash = startupSplash ?: return
        startupSplash = null
        splash.remove()
        android.util.Log.i(
            "EcoPasteStartup",
            "startup timing: stage=android-splash-dismissed atMs=${System.currentTimeMillis()}",
        )
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        EcoPasteShareReceiver.receive(this, intent)
    }

    override fun onResume() {
        super.onResume()
        EcoPasteBridge.setCurrentActivity(this)
        EcoPasteBridge.setForegroundCaptureActive(true)
        EcoPasteBridge.ensureGestureService(this)
        try {
            EcoPasteBridge.notifySyncStatusRefresh()
        } catch (error: Throwable) {
            android.util.Log.w("MainActivity", "refresh sync status warning: ${error.message}")
        }
    }

    override fun onPause() {
        EcoPasteBridge.setForegroundCaptureActive(false)
        super.onPause()
    }

    override fun onDestroy() {
        startupSplash = null
        startupWebView = null
        createDocumentCallback?.invoke(null)
        createDocumentCallback = null
        openDocumentTreeCallback?.invoke(null)
        openDocumentTreeCallback = null
        super.onDestroy()
        EcoPasteBridge.setCurrentActivity(null)
    }

    /** Opens Android's document destination picker for one exported clipboard file. */
    fun createDocument(fileName: String, mimeType: String, callback: (Uri?) -> Unit) {
        if (createDocumentCallback != null || openDocumentTreeCallback != null) {
            EcoPasteBridge.logFileAction("native.save.busy", "another picker is active")
            callback(null)
            return
        }
        createDocumentCallback = callback
        val intent = Intent(Intent.ACTION_CREATE_DOCUMENT).apply {
            addCategory(Intent.CATEGORY_OPENABLE)
            type = mimeType
            putExtra(Intent.EXTRA_TITLE, fileName)
        }
        try {
            EcoPasteBridge.logFileAction(
                "native.save.launch",
                "name=$fileName mime=$mimeType",
            )
            createDocumentLauncher.launch(intent)
        } catch (error: Throwable) {
            createDocumentCallback = null
            callback(null)
            android.util.Log.e(
                "EcoPasteFileAction",
                "native.save.launch.failed | ${error.message}",
                error,
            )
        }
    }

    /** Opens Android's document-tree picker for a remote file download. */
    fun openDocumentTree(callback: (Uri?) -> Unit) {
        if (createDocumentCallback != null || openDocumentTreeCallback != null) {
            EcoPasteBridge.logFileAction("native.save.tree.busy", "another picker is active")
            callback(null)
            return
        }
        openDocumentTreeCallback = callback
        val intent = Intent(Intent.ACTION_OPEN_DOCUMENT_TREE).apply {
            addFlags(
                Intent.FLAG_GRANT_READ_URI_PERMISSION or
                    Intent.FLAG_GRANT_WRITE_URI_PERMISSION or
                    Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION or
                    Intent.FLAG_GRANT_PREFIX_URI_PERMISSION,
            )
        }
        try {
            EcoPasteBridge.logFileAction("native.save.tree.launch", "open document tree")
            openDocumentTreeLauncher.launch(intent)
        } catch (error: Throwable) {
            openDocumentTreeCallback = null
            callback(null)
            android.util.Log.e(
                "EcoPasteFileAction",
                "native.save.tree.launch.failed | ${error.message}",
                error,
            )
        }
    }

    /** 最小化应用但保留后台服务。 */
    fun minimizeToBackground() {
        moveTaskToBack(true)
        @Suppress("DEPRECATION")
        overridePendingTransition(0, 0)
    }
}
