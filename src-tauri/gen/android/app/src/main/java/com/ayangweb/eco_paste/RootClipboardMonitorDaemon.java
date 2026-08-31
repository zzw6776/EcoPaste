package com.ayangweb.eco_paste;

import android.annotation.SuppressLint;
import android.content.ClipData;
import android.content.ClipDescription;
import android.content.ClipboardManager;
import android.content.Context;
import android.os.Build;
import android.os.Looper;
import android.os.PersistableBundle;
import android.os.Process;
import android.util.Base64;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

/** Privileged event-driven clipboard reader launched under Android's system UID. */
public final class RootClipboardMonitorDaemon {
    private static final String WRITEBACK_MARKER = "com.ayangweb.eco_paste.WRITEBACK";

    private RootClipboardMonitorDaemon() {}

    public static void main(String[] args) {
        ClipboardManager manager = null;
        ClipboardManager.OnPrimaryClipChangedListener listener = null;
        try {
            Looper.prepare();
            Looper looper = Looper.myLooper();
            if (looper == null) {
                throw new IllegalStateException("clipboard looper is unavailable");
            }
            Context context = createSystemContext();
            manager = context.getSystemService(ClipboardManager.class);
            if (manager == null) {
                throw new IllegalStateException("clipboard service is unavailable");
            }

            ClipboardManager activeManager = manager;
            listener = () -> emitClipboard(activeManager, context);
            manager.addPrimaryClipChangedListener(listener);
            startStdinWatcher(looper, activeManager, context);

            System.out.println("READY pid=" + Process.myPid());
            System.out.flush();
            emitClipboard(manager, context);
            Looper.loop();
        } catch (Throwable error) {
            System.out.println(
                "ERROR " + error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage())
            );
            error.printStackTrace(System.out);
            System.out.flush();
            System.exit(1);
        } finally {
            if (manager != null && listener != null) {
                manager.removePrimaryClipChangedListener(listener);
            }
        }
    }

    /** Uses the root process system context so Android permits background clipboard reads. */
    private static Context createSystemContext() throws Exception {
        Class<?> activityThreadClass = Class.forName("android.app.ActivityThread");
        Method systemMain = activityThreadClass.getDeclaredMethod("systemMain");
        systemMain.setAccessible(true);
        Object activityThread = systemMain.invoke(null);
        Method getSystemContext = activityThreadClass.getDeclaredMethod("getSystemContext");
        getSystemContext.setAccessible(true);
        return (Context) getSystemContext.invoke(activityThread);
    }

    private static void startStdinWatcher(
        Looper looper,
        ClipboardManager manager,
        Context context
    ) {
        Thread watcher = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if ("CAPTURE".equals(line)) {
                        emitClipboard(manager, context);
                    }
                }
            } catch (IOException ignored) {
                // Closing the controller pipe is the normal shutdown path.
            }
            looper.quitSafely();
        }, "ecopaste-clipboard-stdin");
        watcher.setDaemon(true);
        watcher.start();
    }

    private static void emitClipboard(ClipboardManager manager, Context context) {
        try {
            ClipDescription description = manager.getPrimaryClipDescription();
            if (description == null || isWriteback(description)) {
                return;
            }
            ClipData clip = manager.getPrimaryClip();
            if (clip == null || clip.getItemCount() <= 0) {
                return;
            }
            CharSequence value = clip.getItemAt(0).coerceToText(context);
            if (value == null || value.length() == 0) {
                return;
            }
            long timestamp = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O
                ? description.getTimestamp()
                : 0L;
            String encoded = Base64.encodeToString(
                value.toString().getBytes(StandardCharsets.UTF_8),
                Base64.NO_WRAP
            );
            String sourcePackage = primaryClipSource(manager);
            String encodedSource = Base64.encodeToString(
                sourcePackage.getBytes(StandardCharsets.UTF_8),
                Base64.NO_WRAP
            );
            System.out.println("CLIPBOARD " + timestamp + " " + encodedSource + " " + encoded);
            System.out.flush();
        } catch (Throwable error) {
            System.out.println(
                "CAPTURE_ERROR " + error.getClass().getSimpleName() + ": "
                    + String.valueOf(error.getMessage())
            );
            System.out.flush();
        }
    }

    /** Reads Android's API 30+ clipboard attribution without making it a hard dependency. */
    @SuppressLint("BlockedPrivateApi")
    private static String primaryClipSource(ClipboardManager manager) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) {
            return "";
        }
        try {
            Method method = ClipboardManager.class.getDeclaredMethod("getPrimaryClipSource");
            method.setAccessible(true);
            Object source = method.invoke(manager);
            return source instanceof String ? (String) source : "";
        } catch (Throwable ignored) {
            return "";
        }
    }

    private static boolean isWriteback(ClipDescription description) {
        PersistableBundle extras = description.getExtras();
        return extras != null && extras.getBoolean(WRITEBACK_MARKER, false);
    }
}
