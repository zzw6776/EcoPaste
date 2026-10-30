package com.ayangweb.eco_paste

import android.content.Context
import android.net.TetheringInterface
import android.net.TetheringManager
import androidx.annotation.DoNotInline
import androidx.annotation.RequiresApi
import androidx.core.content.ContextCompat

/** Isolates Android 16 tethering APIs so older systems never resolve their platform types. */
@RequiresApi(36)
internal object TetheringMonitorApi36 {
    @DoNotInline
    fun register(context: Context, onInterfacesChanged: (Set<String>) -> Unit): Any {
        val manager = requireNotNull(context.getSystemService(TetheringManager::class.java))
        val callback = object : TetheringManager.TetheringEventCallback {
            override fun onTetheredInterfacesChanged(interfaces: MutableSet<TetheringInterface>) {
                val interfaceNames = interfaces
                    .filter { it.getType() == TetheringManager.TETHERING_WIFI }
                    .map { it.getInterface() }
                    .filter { it.isNotBlank() }
                    .toSet()
                onInterfacesChanged(interfaceNames)
            }
        }
        manager.registerTetheringEventCallback(ContextCompat.getMainExecutor(context), callback)
        return callback
    }
}
