package com.ayangweb.eco_paste

import android.accessibilityservice.AccessibilityService
import android.content.ClipboardManager
import android.content.Context
import android.os.Build
import android.util.Log
import android.view.accessibility.AccessibilityEvent
import android.view.accessibility.AccessibilityNodeInfo

/** 无障碍模式只负责剪贴板辅助读取和模拟粘贴，不再承载上滑手势。 */
class EcoPasteAccessibilityService : AccessibilityService() {

    companion object {
        private const val TAG = "EcoPasteAccessibility"
        var instance: EcoPasteAccessibilityService? = null
            private set
    }

    private var clipboardManager: ClipboardManager? = null
    private val clipChangedListener = ClipboardManager.OnPrimaryClipChangedListener {
        if (EcoPasteBridge.currentEngineMode == "accessibility") {
            EcoPasteBridge.captureClipboardChange(clipboardManager, true)
        }
    }

    override fun onServiceConnected() {
        super.onServiceConnected()
        instance = this
        EcoPasteBridge.initialize(applicationContext)
        Log.i(TAG, "EcoPasteAccessibilityService connected")

        try {
            clipboardManager = getSystemService(Context.CLIPBOARD_SERVICE) as? ClipboardManager
            clipboardManager?.addPrimaryClipChangedListener(clipChangedListener)
        } catch (error: Exception) {
            Log.e(TAG, "failed to register clipboard listener: ${error.message}", error)
        }
    }

    override fun onAccessibilityEvent(event: AccessibilityEvent?) {
        if (event == null || EcoPasteBridge.currentEngineMode != "accessibility") return

        val sourcePackage = event.packageName?.toString()
        EcoPasteBridge.recordAccessibilitySource(sourcePackage)

        when (event.eventType) {
            AccessibilityEvent.TYPE_VIEW_CLICKED,
            AccessibilityEvent.TYPE_VIEW_TEXT_SELECTION_CHANGED,
            AccessibilityEvent.TYPE_WINDOW_STATE_CHANGED,
            AccessibilityEvent.TYPE_VIEW_FOCUSED -> {
                EcoPasteBridge.captureClipboardChange(clipboardManager, false, sourcePackage)
            }
        }
    }

    /** 自动在当前前台应用的输入框执行模拟粘贴。 */
    fun performPaste(): Boolean {
        try {
            val root = rootInActiveWindow ?: return false
            val focusedNode = root.findFocus(AccessibilityNodeInfo.FOCUS_INPUT)
            if (focusedNode != null && focusedNode.isEditable) {
                val success = focusedNode.performAction(AccessibilityNodeInfo.ACTION_PASTE)
                recycleNode(focusedNode)
                if (focusedNode !== root) recycleNode(root)
                return success
            }

            val targetNode = findEditableNode(root)
            if (targetNode != null) {
                val success = targetNode.performAction(AccessibilityNodeInfo.ACTION_PASTE)
                recycleNode(targetNode)
                if (targetNode !== root) recycleNode(root)
                return success
            }
            recycleNode(root)
        } catch (error: Exception) {
            Log.e(TAG, "performPaste failed: ${error.message}", error)
        }
        return false
    }

    private fun findEditableNode(node: AccessibilityNodeInfo): AccessibilityNodeInfo? {
        if (node.isEditable && (node.isFocused || node.isSelected)) {
            return node
        }
        for (index in 0 until node.childCount) {
            val child = node.getChild(index) ?: continue
            val result = findEditableNode(child)
            if (result != null) {
                return result
            }
            recycleNode(child)
        }
        return null
    }

    /** Android 13 以前需要显式回收节点；新版本由系统自动管理。 */
    @Suppress("DEPRECATION")
    private fun recycleNode(node: AccessibilityNodeInfo) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            node.recycle()
        }
    }

    override fun onInterrupt() {
        Log.w(TAG, "EcoPasteAccessibilityService interrupted")
    }

    override fun onDestroy() {
        try {
            clipboardManager?.removePrimaryClipChangedListener(clipChangedListener)
        } catch (_: Exception) {}
        clipboardManager = null
        instance = null
        super.onDestroy()
        Log.i(TAG, "EcoPasteAccessibilityService destroyed")
    }
}
