package com.ayangweb.eco_paste

/** Recognizes Android Wi-Fi client and hotspot interface names without accepting virtual routes. */
internal fun isLikelyHotspotInterfaceName(name: String): Boolean {
    val normalized = name.lowercase()
    return normalized.matches(Regex("wlan\\d+")) ||
        normalized.matches(Regex("ap\\d+")) ||
        normalized.startsWith("ap_br_wlan") ||
        normalized.startsWith("softap") ||
        normalized.startsWith("swlan") ||
        normalized.startsWith("wifi-ap")
}
