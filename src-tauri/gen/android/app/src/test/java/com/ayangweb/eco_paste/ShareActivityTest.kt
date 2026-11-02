package com.ayangweb.eco_paste

import android.content.Intent
import org.junit.Assert.assertEquals
import org.junit.Test

class ShareActivityTest {
    @Test
    fun removesFileManagerTaskFlagsWhileKeepingContentGrants() {
        // Flags observed on the failing system file-manager delivery.
        val forwarded = shareForwardingFlags(0x1b088003)
        assertEquals(
            Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP or
                Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION,
            forwarded,
        )
        assertEquals(0, forwarded and Intent.FLAG_ACTIVITY_CLEAR_TASK)
        assertEquals(0, forwarded and Intent.FLAG_ACTIVITY_NEW_DOCUMENT)
        assertEquals(0, forwarded and Intent.FLAG_ACTIVITY_MULTIPLE_TASK)
    }

    @Test
    fun preservesPrefixAndPersistableUriGrants() {
        val grants = Intent.FLAG_GRANT_READ_URI_PERMISSION or
            Intent.FLAG_GRANT_PREFIX_URI_PERMISSION or Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION
        assertEquals(
            grants or Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP,
            shareForwardingFlags(grants),
        )
    }
}
