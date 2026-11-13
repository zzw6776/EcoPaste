package com.ayangweb.eco_paste

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.util.Log

internal const val SHARE_SOURCE_PACKAGE = "com.ayangweb.eco_paste.SHARE_SOURCE_PACKAGE"

/** Captures the sender before forwarding the share into our own main activity. */
internal fun shareSourcePackage(activity: Activity): String? {
    val packageName = activity.callingPackage
        ?: activity.referrer?.takeIf { it.scheme == "android-app" }?.host
    return packageName?.takeIf { it.isNotBlank() && it != activity.packageName }
}

/** Preserves URI grants while replacing every sender-controlled activity/task flag. */
internal fun shareForwardingFlags(flags: Int): Int {
    val grants = Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION or
        Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION or Intent.FLAG_GRANT_PREFIX_URI_PERMISSION
    return (flags and grants) or Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP
}

/** Isolates sender task flags from the Tauri activity while forwarding content grants. */
class ShareActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val delivery = intent
        if (delivery?.action != Intent.ACTION_SEND && delivery?.action != Intent.ACTION_SEND_MULTIPLE) {
            finish()
            return
        }

        val forwarded = Intent(delivery).apply {
            putExtra(SHARE_SOURCE_PACKAGE, shareSourcePackage(this@ShareActivity))
            setClass(this@ShareActivity, MainActivity::class.java)
            // CLEAR_TASK from some file managers would destroy the running Rust event loop.
            flags = shareForwardingFlags(delivery.flags)
        }
        try {
            startActivity(forwarded)
        } catch (error: Exception) {
            Log.e("EcoPasteShare", "Unable to forward share delivery", error)
        } finally {
            finish()
        }
    }
}
