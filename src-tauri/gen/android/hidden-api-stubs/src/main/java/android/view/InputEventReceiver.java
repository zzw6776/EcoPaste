package android.view;

import android.os.Looper;

/** Compile-only signatures for the framework hidden API. */
public abstract class InputEventReceiver {
    public InputEventReceiver(InputChannel inputChannel, Looper looper) {}

    public void onInputEvent(InputEvent event) {}

    public final void finishInputEvent(InputEvent event, boolean handled) {}

    public void dispose() {}
}
