package com.ayangweb.eco_paste

import android.content.Context
import android.util.Log
import java.io.BufferedWriter
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * 在独立 root app_process 中启动全局 InputMonitor。
 *
 * 普通输入只观察；只有 daemon 确认底角上滑并 pilfer 后，才通知应用唤起窗口。
 */
class RootInputMonitor(
    context: Context,
    private val onReady: (Boolean) -> Unit,
    private val onSwipe: () -> Unit,
    private val onBackSwipe: (Long?) -> Unit,
    private val onHomeSwipe: (Long?) -> Unit,
) {
    companion object {
        private const val TAG = "EcoPasteRootInput"
        private const val DAEMON_CLASS = "com.ayangweb.eco_paste.RootInputMonitorDaemon"
    }

    private val applicationContext = context.applicationContext
    private val generation = AtomicInteger(0)
    private val writerLock = Any()

    @Volatile
    private var panelSessionId: Long? = null

    @Volatile
    private var daemonWriter: BufferedWriter? = null

    @Volatile
    private var process: Process? = null

    @Volatile
    private var daemonPid: Int? = null

    @Volatile
    private var worker: Thread? = null

    /** 启动与当前显示尺寸绑定的 root 输入监控。 */
    fun start(
        displayId: Int,
        displayWidth: Int,
        displayHeight: Int,
        leftSensorWidth: Int,
        leftSensorHeight: Int,
        rightSensorWidth: Int,
        rightSensorHeight: Int,
        systemGestureHeight: Int,
        swipeThreshold: Float,
        maxDurationMs: Long,
        panelTop: Int,
        backEdgeWidth: Int,
        backThreshold: Float,
    ) {
        stop()
        val runId = generation.incrementAndGet()
        worker = thread(name = "ecopaste-root-input", start = true) {
            runDaemon(
                runId,
                displayId,
                displayWidth,
                displayHeight,
                leftSensorWidth,
                leftSensorHeight,
                rightSensorWidth,
                rightSensorHeight,
                systemGestureHeight,
                swipeThreshold,
                maxDurationMs,
                panelTop,
                backEdgeWidth,
                backThreshold,
            )
        }
    }

    /** 同步当前面板会话，Root daemon 仅为同一会话处理收起手势。 */
    fun setPanelSession(sessionId: Long?) {
        panelSessionId = sessionId
        sendPanelState()
    }

    /** 停止当前 daemon；关闭 stdin 让 daemon 自行退出，并按已确认 PID 补充终止。 */
    fun stop() {
        generation.incrementAndGet()
        val currentProcess = process
        process = null
        val currentPid = daemonPid
        daemonPid = null
        worker?.interrupt()
        worker = null

        synchronized(writerLock) {
            try {
                daemonWriter?.close()
            } catch (_: Exception) {}
            daemonWriter = null
        }
        try {
            currentProcess?.inputStream?.close()
        } catch (_: Exception) {}
        currentProcess?.destroy()

        if (currentPid != null) {
            thread(name = "ecopaste-root-input-stop", start = true) {
                stopDaemon(currentPid)
            }
        }
    }

    private fun runDaemon(
        runId: Int,
        displayId: Int,
        displayWidth: Int,
        displayHeight: Int,
        leftSensorWidth: Int,
        leftSensorHeight: Int,
        rightSensorWidth: Int,
        rightSensorHeight: Int,
        systemGestureHeight: Int,
        swipeThreshold: Float,
        maxDurationMs: Long,
        panelTop: Int,
        backEdgeWidth: Int,
        backThreshold: Float,
    ) {
        try {
            val apkPath = applicationContext.applicationInfo.sourceDir
            val command = buildString {
                append("CLASSPATH=")
                append(shellQuote(apkPath))
                append(" exec app_process -Xhidden-api-policy:disabled /system/bin ")
                append(DAEMON_CLASS)
                append(" --owner-pid ").append(android.os.Process.myPid())
                append(" --display-id ").append(displayId)
                append(" --display-width ").append(displayWidth)
                append(" --display-height ").append(displayHeight)
                append(" --left-sensor-width ").append(leftSensorWidth)
                append(" --left-sensor-height ").append(leftSensorHeight)
                append(" --right-sensor-width ").append(rightSensorWidth)
                append(" --right-sensor-height ").append(rightSensorHeight)
                append(" --system-gesture-height ").append(systemGestureHeight)
                append(" --swipe-threshold ").append(swipeThreshold)
                append(" --max-duration-ms ").append(maxDurationMs)
                append(" --panel-top ").append(panelTop)
                append(" --back-edge-width ").append(backEdgeWidth)
                append(" --back-threshold ").append(backThreshold)
            }
            val daemon = ProcessBuilder("su", "-c", command)
                .redirectErrorStream(true)
                .start()
            if (generation.get() != runId) {
                daemon.destroy()
                return
            }
            process = daemon
            synchronized(writerLock) {
                daemonWriter = daemon.outputStream.bufferedWriter()
            }

            daemon.inputStream.bufferedReader().use { reader ->
                while (generation.get() == runId) {
                    val line = reader.readLine() ?: break
                    when {
                        line.startsWith("READY ") -> {
                            daemonPid = parseDaemonPid(line)
                            Log.i(TAG, line)
                            sendPanelState()
                            onReady(true)
                        }
                        line.startsWith("PILFERED ") -> {
                            Log.i(TAG, line)
                            onSwipe()
                        }
                        line.startsWith("PILFERED_BACK ") -> {
                            Log.i(TAG, line)
                            onBackSwipe(parseSessionId(line))
                        }
                        line.startsWith("DISMISS_HOME ") -> {
                            Log.i(TAG, line)
                            onHomeSwipe(parseSessionId(line))
                        }
                        line.startsWith("ERROR ") -> Log.e(TAG, line)
                        else -> Log.d(TAG, "daemon: $line")
                    }
                }
            }

            val exitCode = daemon.waitFor()
            if (generation.get() == runId) {
                Log.w(TAG, "root input daemon exited with code=$exitCode")
                onReady(false)
            }
        } catch (error: Exception) {
            if (generation.get() == runId) {
                Log.w(TAG, "root input daemon failed: ${error.message}", error)
                onReady(false)
            }
        } finally {
            if (generation.get() == runId) {
                process = null
                daemonPid = null
                worker = null
                synchronized(writerLock) {
                    daemonWriter = null
                }
            }
        }
    }

    private fun sendPanelState() {
        synchronized(writerLock) {
            try {
                daemonWriter?.apply {
                    write("PANEL ${panelSessionId ?: 0L}")
                    newLine()
                    flush()
                }
            } catch (error: Exception) {
                Log.d(TAG, "send panel state ignored: ${error.message}")
            }
        }
    }

    private fun parseDaemonPid(line: String): Int? {
        return Regex("""(?:^|\s)pid=(\d+)(?:\s|$)""")
            .find(line)
            ?.groupValues
            ?.get(1)
            ?.toIntOrNull()
    }

    private fun parseSessionId(line: String): Long? {
        return Regex("""(?:^|\s)session=(\d+)(?:\s|$)""")
            .find(line)
            ?.groupValues
            ?.get(1)
            ?.toLongOrNull()
            ?.takeIf { it > 0L }
    }

    private fun stopDaemon(pid: Int) {
        try {
            ProcessBuilder("su", "-c", "kill $pid")
                .redirectErrorStream(true)
                .start()
                .waitFor()
        } catch (error: Exception) {
            Log.d(TAG, "root input daemon stop ignored: ${error.message}")
        }
    }

    private fun shellQuote(value: String): String {
        return "'${value.replace("'", "'\\''")}'"
    }
}
