package com.ayangweb.eco_paste

import android.Manifest
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.webkit.WebView
import androidx.activity.OnBackPressedCallback
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

class MainActivity : TauriActivity() {
    override val handleBackNavigation: Boolean = false

    private var createDocumentCallback: ((Uri?) -> Unit)? = null
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

    private external fun initNdkContext(context: Context)

    override fun onWebViewCreate(webView: WebView) {
        super.onWebViewCreate(webView)
        onBackPressedDispatcher.addCallback(
            this,
            object : OnBackPressedCallback(true) {
                override fun handleOnBackPressed() {
                    if (webView.canGoBack()) {
                        webView.goBack()
                    } else {
                        minimizeToBackground()
                    }
                }
            },
        )
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
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

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
            ContextCompat.checkSelfPermission(
                this,
                Manifest.permission.POST_NOTIFICATIONS,
            ) != PackageManager.PERMISSION_GRANTED
        ) {
            ActivityCompat.requestPermissions(
                this,
                arrayOf(Manifest.permission.POST_NOTIFICATIONS),
                101,
            )
        }

        EcoPasteBridge.ensureGestureService(this)
    }

    override fun onResume() {
        super.onResume()
        EcoPasteBridge.setCurrentActivity(this)
        EcoPasteBridge.setForegroundCaptureActive(true)
        try {
            EcoPasteBridge.notifySyncForeground()
        } catch (error: Throwable) {
            android.util.Log.w("MainActivity", "notify sync foreground warning: ${error.message}")
        }
    }

    override fun onPause() {
        EcoPasteBridge.setForegroundCaptureActive(false)
        super.onPause()
    }

    override fun onDestroy() {
        createDocumentCallback?.invoke(null)
        createDocumentCallback = null
        super.onDestroy()
        EcoPasteBridge.setCurrentActivity(null)
    }

    /** Opens Android's document destination picker for one exported clipboard file. */
    fun createDocument(fileName: String, mimeType: String, callback: (Uri?) -> Unit) {
        if (createDocumentCallback != null) {
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

    /** 最小化应用但保留后台服务。 */
    fun minimizeToBackground() {
        moveTaskToBack(true)
        @Suppress("DEPRECATION")
        overridePendingTransition(0, 0)
    }

}
