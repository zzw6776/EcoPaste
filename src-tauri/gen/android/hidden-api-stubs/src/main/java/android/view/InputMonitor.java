package android.view;

/** Compile-only signatures for the framework hidden API. */
public final class InputMonitor {
    public InputChannel getInputChannel() {
        throw new UnsupportedOperationException("framework stub");
    }

    public void pilferPointers() {
        throw new UnsupportedOperationException("framework stub");
    }

    public void dispose() {
        throw new UnsupportedOperationException("framework stub");
    }
}
