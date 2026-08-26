package com.ayangweb.eco_paste

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import androidx.activity.OnBackPressedCallback
import androidx.activity.enableEdgeToEdge
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat

class MainActivity : TauriActivity() {
    private external fun initNdkContext(context: Context)

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        try {
            initNdkContext(applicationContext)
            EcoPasteBridge.initNdkContext(applicationContext)
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

        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                minimizeToBackground()
            }
        })

        EcoPasteBridge.ensureGestureService(this)
    }

    override fun onResume() {
        super.onResume()
        EcoPasteBridge.setCurrentActivity(this)
        try {
            EcoPasteBridge.notifySyncNetworkChanged()
        } catch (error: Throwable) {
            android.util.Log.w("MainActivity", "notify sync foreground warning: ${error.message}")
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        EcoPasteBridge.setCurrentActivity(null)
    }

    /** 最小化应用但保留后台服务。 */
    fun minimizeToBackground() {
        moveTaskToBack(true)
        @Suppress("DEPRECATION")
        overridePendingTransition(0, 0)
    }
}
