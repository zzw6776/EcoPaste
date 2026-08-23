package com.ayangweb.eco_paste;

import android.hardware.input.InputManagerGlobal;
import android.os.Handler;
import android.os.Looper;
import android.os.Process;
import android.os.SystemClock;
import android.view.InputEvent;
import android.view.InputEventReceiver;
import android.view.InputMonitor;
import android.view.MotionEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/** Root-only SPY InputMonitor entrypoint launched through app_process. */
public final class RootInputMonitorDaemon {
    private static final String MONITOR_NAME = "EcoPasteRootGesture";

    private RootInputMonitorDaemon() {}

    public static void main(String[] args) {
        try {
            Config config = Config.parse(args);
            Looper.prepare();
            Looper looper = Looper.myLooper();
            if (looper == null) {
                throw new IllegalStateException("input looper is unavailable");
            }

            InputMonitor inputMonitor = InputManagerGlobal.getInstance()
                .monitorGestureInput(MONITOR_NAME, config.displayId);
            GestureReceiver receiver = new GestureReceiver(inputMonitor, looper, config);
            startOwnerWatcher(looper, config.ownerPid);
            startStdinWatcher(looper, receiver);

            System.out.println("READY pid=" + Process.myPid());
            Looper.loop();

            receiver.dispose();
            inputMonitor.dispose();
        } catch (Throwable error) {
            System.out.println(
                "ERROR " + error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage())
            );
            error.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static void startOwnerWatcher(Looper looper, int ownerPid) {
        Thread watcher = new Thread(() -> {
            File owner = new File("/proc/" + ownerPid);
            while (owner.exists()) {
                try {
                    Thread.sleep(1_000L);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            looper.quitSafely();
        }, "ecopaste-owner-watch");
        watcher.setDaemon(true);
        watcher.start();
    }

    private static void startStdinWatcher(Looper looper, GestureReceiver receiver) {
        Thread watcher = new Thread(() -> {
            Handler handler = new Handler(looper);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("PANEL ")) {
                        long sessionId = parsePanelSession(line);
                        handler.post(() -> receiver.setPanelSession(sessionId));
                    }
                }
            } catch (IOException ignored) {
                // Closing the controller pipe is the normal shutdown path.
            }
            looper.quitSafely();
        }, "ecopaste-stdin-watch");
        watcher.setDaemon(true);
        watcher.start();
    }

    private static long parsePanelSession(String line) {
        try {
            return Math.max(0L, Long.parseLong(line.substring("PANEL ".length()).trim()));
        } catch (RuntimeException ignored) {
            return 0L;
        }
    }

    private static final class GestureReceiver extends InputEventReceiver {
        private final InputMonitor inputMonitor;
        private final Config config;
        private static final int CANDIDATE_NONE = 0;
        private static final int CANDIDATE_WAKE = 1;
        private static final int CANDIDATE_BACK_LEFT = 2;
        private static final int CANDIDATE_BACK_RIGHT = 3;
        private static final int CANDIDATE_HOME_DISMISS = 4;
        private int candidate = CANDIDATE_NONE;
        private boolean pilfered;
        private boolean wakeTriggered;
        private long panelSessionId;
        private float startX;
        private float startY;
        private long startAt;

        GestureReceiver(InputMonitor inputMonitor, Looper looper, Config config) {
            super(inputMonitor.getInputChannel(), looper);
            this.inputMonitor = inputMonitor;
            this.config = config;
        }

        void setPanelSession(long sessionId) {
            panelSessionId = sessionId;
            candidate = CANDIDATE_NONE;
            pilfered = false;
            wakeTriggered = false;
        }

        @Override
        public void onInputEvent(InputEvent inputEvent) {
            try {
                if (inputEvent instanceof MotionEvent) {
                    handleMotionEvent((MotionEvent) inputEvent);
                }
            } finally {
                finishInputEvent(inputEvent, true);
            }
        }

        private void handleMotionEvent(MotionEvent event) {
            switch (event.getActionMasked()) {
                case MotionEvent.ACTION_DOWN:
                    startX = event.getRawX();
                    startY = event.getRawY();
                    startAt = SystemClock.uptimeMillis();
                    if (config.isInSensor(startX, startY)) {
                        candidate = CANDIDATE_WAKE;
                    } else if (panelSessionId > 0L && config.isInSystemGestureArea(startY)) {
                        candidate = CANDIDATE_HOME_DISMISS;
                    } else if (panelSessionId > 0L && config.isInBackEdge(startX)) {
                        if (startX <= config.backEdgeWidth) {
                            candidate = CANDIDATE_BACK_LEFT;
                        } else {
                            candidate = CANDIDATE_BACK_RIGHT;
                        }
                    } else {
                        candidate = CANDIDATE_NONE;
                    }
                    pilfered = false;
                    wakeTriggered = false;
                    if (isBackCandidate()) {
                        inputMonitor.pilferPointers();
                    }
                    break;
                case MotionEvent.ACTION_POINTER_DOWN:
                    candidate = CANDIDATE_NONE;
                    break;
                case MotionEvent.ACTION_MOVE:
                    maybePilfer(event);
                    break;
                case MotionEvent.ACTION_UP:
                    if (isBackCandidate() && !pilfered) {
                        emitPanelDismiss("PILFERED_BACK reason=release");
                    }
                    resetGesture();
                    break;
                case MotionEvent.ACTION_CANCEL:
                    if (candidate == CANDIDATE_HOME_DISMISS) {
                        emitPanelDismiss("DISMISS_HOME reason=cancel");
                    } else if (isBackCandidate() && !pilfered) {
                        emitPanelDismiss("PILFERED_BACK reason=cancel");
                    }
                    resetGesture();
                    break;
                default:
                    break;
            }
        }

        private void maybePilfer(MotionEvent event) {
            if (candidate == CANDIDATE_NONE) {
                return;
            }

            long duration = SystemClock.uptimeMillis() - startAt;
            if (duration > config.maxDurationMs) {
                candidate = CANDIDATE_NONE;
                return;
            }

            float signedDeltaX = event.getRawX() - startX;
            float deltaX = Math.abs(signedDeltaX);
            float absoluteDeltaY = Math.abs(event.getRawY() - startY);
            if (candidate == CANDIDATE_HOME_DISMISS) {
                float deltaY = startY - event.getRawY();
                if (deltaY <= config.swipeThreshold || deltaY <= deltaX) {
                    return;
                }

                candidate = CANDIDATE_NONE;
                emitPanelDismiss(
                    "DISMISS_HOME deltaX=" + deltaX + " deltaY=" + deltaY + " duration=" + duration
                );
                return;
            }

            if (candidate == CANDIDATE_BACK_LEFT || candidate == CANDIDATE_BACK_RIGHT) {
                if (pilfered) {
                    return;
                }
                boolean inward = candidate == CANDIDATE_BACK_LEFT ? signedDeltaX > 0 : signedDeltaX < 0;
                if (!inward || deltaX <= config.backThreshold || deltaX <= absoluteDeltaY) {
                    return;
                }

                inputMonitor.pilferPointers();
                pilfered = true;
                emitPanelDismiss(
                    "PILFERED_BACK deltaX=" + signedDeltaX
                        + " deltaY=" + (event.getRawY() - startY)
                        + " duration=" + duration
                );
                return;
            }

            float deltaY = startY - event.getRawY();
            if (wakeTriggered || deltaY <= config.swipeThreshold || deltaY <= deltaX) {
                return;
            }

            inputMonitor.pilferPointers();
            pilfered = true;
            wakeTriggered = true;
            System.out.println(
                "PILFERED deltaX=" + deltaX + " deltaY=" + deltaY + " duration=" + duration
            );
        }

        private void resetGesture() {
            candidate = CANDIDATE_NONE;
            pilfered = false;
            wakeTriggered = false;
        }

        private boolean isBackCandidate() {
            return candidate == CANDIDATE_BACK_LEFT || candidate == CANDIDATE_BACK_RIGHT;
        }

        private void emitPanelDismiss(String message) {
            long dismissedSessionId = panelSessionId;
            panelSessionId = 0L;
            System.out.println(message + " session=" + dismissedSessionId);
        }

    }

    private static final class Config {
        final int ownerPid;
        final int displayId;
        final int displayWidth;
        final int displayHeight;
        final int leftSensorWidth;
        final int leftSensorHeight;
        final int rightSensorWidth;
        final int rightSensorHeight;
        final int systemGestureHeight;
        final float swipeThreshold;
        final long maxDurationMs;
        final int panelTop;
        final int backEdgeWidth;
        final float backThreshold;

        Config(
            int ownerPid,
            int displayId,
            int displayWidth,
            int displayHeight,
            int leftSensorWidth,
            int leftSensorHeight,
            int rightSensorWidth,
            int rightSensorHeight,
            int systemGestureHeight,
            float swipeThreshold,
            long maxDurationMs,
            int panelTop,
            int backEdgeWidth,
            float backThreshold
        ) {
            this.ownerPid = ownerPid;
            this.displayId = displayId;
            this.displayWidth = displayWidth;
            this.displayHeight = displayHeight;
            this.leftSensorWidth = leftSensorWidth;
            this.leftSensorHeight = leftSensorHeight;
            this.rightSensorWidth = rightSensorWidth;
            this.rightSensorHeight = rightSensorHeight;
            this.systemGestureHeight = systemGestureHeight;
            this.swipeThreshold = swipeThreshold;
            this.maxDurationMs = maxDurationMs;
            this.panelTop = panelTop;
            this.backEdgeWidth = backEdgeWidth;
            this.backThreshold = backThreshold;
        }

        boolean isInPanel(float y) {
            return y >= panelTop && y <= displayHeight;
        }

        boolean isInBackEdge(float x) {
            return backEdgeWidth > 0
                && (x <= backEdgeWidth || x >= displayWidth - backEdgeWidth);
        }

        boolean isInSensor(float x, float y) {
            boolean inLeft = leftSensorWidth > 0
                && leftSensorHeight > 0
                && x <= leftSensorWidth
                && y >= displayHeight - leftSensorHeight
                && y <= displayHeight;
            boolean inRight = rightSensorWidth > 0
                && rightSensorHeight > 0
                && x >= displayWidth - rightSensorWidth
                && y >= displayHeight - rightSensorHeight
                && y <= displayHeight;
            return inLeft || inRight;
        }

        boolean isInSystemGestureArea(float y) {
            return systemGestureHeight > 0
                && y >= displayHeight - systemGestureHeight
                && y <= displayHeight;
        }

        static Config parse(String[] args) {
            int ownerPid = intArg(args, "--owner-pid");
            int displayId = intArg(args, "--display-id");
            int displayWidth = positiveIntArg(args, "--display-width");
            int displayHeight = positiveIntArg(args, "--display-height");
            int leftSensorWidth = nonNegativeIntArg(args, "--left-sensor-width");
            int leftSensorHeight = nonNegativeIntArg(args, "--left-sensor-height");
            int rightSensorWidth = nonNegativeIntArg(args, "--right-sensor-width");
            int rightSensorHeight = nonNegativeIntArg(args, "--right-sensor-height");
            int systemGestureHeight = positiveIntArg(args, "--system-gesture-height");
            float swipeThreshold = Float.parseFloat(value(args, "--swipe-threshold"));
            long maxDurationMs = Long.parseLong(value(args, "--max-duration-ms"));
            int panelTop = nonNegativeIntArg(args, "--panel-top");
            int backEdgeWidth = nonNegativeIntArg(args, "--back-edge-width");
            float backThreshold = Float.parseFloat(value(args, "--back-threshold"));
            return new Config(
                ownerPid,
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
                backThreshold
            );
        }

        private static int positiveIntArg(String[] args, String name) {
            int result = intArg(args, name);
            if (result <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return result;
        }

        private static int nonNegativeIntArg(String[] args, String name) {
            int result = intArg(args, name);
            if (result < 0) {
                throw new IllegalArgumentException(name + " must not be negative");
            }
            return result;
        }

        private static int intArg(String[] args, String name) {
            return Integer.parseInt(value(args, name));
        }

        private static String value(String[] args, String name) {
            for (int index = 0; index < args.length - 1; index += 2) {
                if (name.equals(args[index])) {
                    return args[index + 1];
                }
            }
            throw new IllegalArgumentException("missing argument " + name);
        }
    }
}
