package com.ayangweb.eco_paste

import android.content.Context
import android.util.Base64
import android.util.Log
import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/** Controls the root clipboard daemon; retries happen only after the daemon exits. */
class RootClipboardMonitor(
    context: Context,
    private val onClipboard: (String, Long, String?) -> Unit,
) {
    companion object {
        private const val TAG = "EcoPasteRootClipboard"
        private const val DAEMON_CLASS = "com.ayangweb.eco_paste.RootClipboardMonitorDaemon"
        private val RETRY_DELAYS_MS = longArrayOf(2_000L, 5_000L, 15_000L, 30_000L, 60_000L, 300_000L)
    }

    private val applicationContext = context.applicationContext
    private val generation = AtomicInteger(0)
    private val writerLock = Any()

    @Volatile
    private var daemonWriter: BufferedWriter? = null

    @Volatile
    private var process: Process? = null

    @Volatile
    private var daemonPid: Int? = null

    @Volatile
    private var worker: Thread? = null

    fun start() {
        if (worker?.isAlive == true) return
        val runId = generation.incrementAndGet()
        worker = thread(name = "ecopaste-root-clipboard", start = true) {
            runLoop(runId)
        }
    }

    /** Requests one snapshot after the Rust runtime becomes ready or returns to foreground. */
    fun requestCapture() {
        synchronized(writerLock) {
            try {
                daemonWriter?.apply {
                    write("CAPTURE")
                    newLine()
                    flush()
                }
            } catch (error: Exception) {
                Log.d(TAG, "request clipboard snapshot ignored: ${error.message}")
            }
        }
    }

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
            thread(name = "ecopaste-root-clipboard-stop", start = true) {
                stopDaemon(currentPid)
            }
        }
    }

    private fun runLoop(runId: Int) {
        var failureCount = 0
        while (generation.get() == runId) {
            val ready = runDaemon(runId)
            if (generation.get() != runId) break
            if (ready) failureCount = 0
            val delay = RETRY_DELAYS_MS.getOrNull(failureCount++) ?: break
            Log.w(TAG, "root clipboard daemon retry in ${delay}ms")
            try {
                Thread.sleep(delay)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }
        if (generation.get() == runId) {
            worker = null
            Log.w(TAG, "root clipboard daemon retry suspended until the next resume event")
        }
    }

    private fun runDaemon(runId: Int): Boolean {
        var readyAt = 0L
        try {
            val apkPath = applicationContext.applicationInfo.sourceDir
            val command = buildString {
                append("CLASSPATH=")
                append(shellQuote(apkPath))
                append(" exec app_process -Xhidden-api-policy:disabled /system/bin ")
                append(DAEMON_CLASS)
            }
            // ClipboardService validates that the Binder UID owns the system context package.
            // Magisk starts as root by default, so explicitly use Android's system UID.
            val daemon = ProcessBuilder("su", "1000", "-c", command)
                .redirectErrorStream(true)
                .start()
            if (generation.get() != runId) {
                daemon.destroy()
                return false
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
                            readyAt = System.nanoTime()
                            daemonPid = parseDaemonPid(line)
                            Log.i(TAG, line)
                        }
                        line.startsWith("CLIPBOARD ") -> dispatchClipboard(line)
                        line.startsWith("CAPTURE_ERROR ") -> Log.w(TAG, line)
                        line.startsWith("ERROR ") -> Log.e(TAG, line)
                        else -> Log.d(TAG, "daemon: $line")
                    }
                }
            }
            val exitCode = daemon.waitFor()
            if (generation.get() == runId) {
                Log.w(TAG, "root clipboard daemon exited with code=$exitCode")
            }
        } catch (error: Exception) {
            if (generation.get() == runId) {
                Log.w(TAG, "root clipboard daemon failed: ${error.message}", error)
            }
        } finally {
            if (generation.get() == runId) {
                process = null
                daemonPid = null
                synchronized(writerLock) {
                    daemonWriter = null
                }
            }
        }
        return readyAt > 0L && System.nanoTime() - readyAt >= 30_000_000_000L
    }

    private fun dispatchClipboard(line: String) {
        val parts = line.split(' ', limit = 4)
        if (parts.size != 4) return
        val timestamp = parts[1].toLongOrNull() ?: 0L
        try {
            val sourcePackage = String(
                Base64.decode(parts[2], Base64.DEFAULT),
                StandardCharsets.UTF_8,
            ).ifBlank { null }
            val text = String(Base64.decode(parts[3], Base64.DEFAULT), StandardCharsets.UTF_8)
            onClipboard(text, timestamp, sourcePackage)
        } catch (error: Exception) {
            Log.w(TAG, "decode root clipboard event failed: ${error.message}")
        }
    }

    private fun parseDaemonPid(line: String): Int? {
        return Regex("""(?:^|\s)pid=(\d+)(?:\s|$)""")
            .find(line)
            ?.groupValues
            ?.get(1)
            ?.toIntOrNull()
    }

    private fun stopDaemon(pid: Int) {
        try {
            ProcessBuilder("su", "-c", "kill $pid")
                .redirectErrorStream(true)
                .start()
                .waitFor()
        } catch (error: Exception) {
            Log.d(TAG, "root clipboard daemon stop ignored: ${error.message}")
        }
    }

    private fun shellQuote(value: String): String {
        return "'${value.replace("'", "'\\''")}'"
    }
}
