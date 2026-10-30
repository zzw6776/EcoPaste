package com.ayangweb.eco_paste

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class LanInterfaceClassifierTest {
    @Test
    fun acceptsAndroidWifiAndHotspotInterfaces() {
        listOf("wlan0", "wlan2", "ap0", "ap_br_wlan2", "softap0", "swlan0", "wifi-ap0")
            .forEach { assertTrue(it, isLikelyHotspotInterfaceName(it)) }
    }

    @Test
    fun rejectsCellularVpnAndVirtualInterfaces() {
        listOf("rmnet_data0", "tun0", "wg0", "docker0", "br0", "veth1234", "dummy0")
            .forEach { assertFalse(it, isLikelyHotspotInterfaceName(it)) }
    }
}
