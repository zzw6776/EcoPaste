package android.hardware.input;

import android.view.InputMonitor;

/** Compile-only signatures for the framework hidden API. */
public final class InputManagerGlobal {
    public static InputManagerGlobal getInstance() {
        throw new UnsupportedOperationException("framework stub");
    }

    public InputMonitor monitorGestureInput(String name, int displayId) {
        throw new UnsupportedOperationException("framework stub");
    }
}
