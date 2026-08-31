package com.ayangweb.eco_paste

import android.content.Context
import android.content.res.ColorStateList
import android.content.res.Configuration
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Color
import android.graphics.PixelFormat
import android.graphics.Rect
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.provider.Settings
import android.text.Editable
import android.text.TextWatcher
import android.util.Log
import android.view.Gravity
import android.view.MotionEvent
import android.view.View
import android.view.WindowInsets
import android.view.WindowManager
import android.view.animation.AlphaAnimation
import android.view.animation.Animation
import android.view.inputmethod.InputMethodManager
import android.widget.EditText
import android.widget.FrameLayout
import android.widget.HorizontalScrollView
import android.widget.ImageButton
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.PopupMenu
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import org.json.JSONArray
import org.json.JSONObject
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.math.abs
import kotlin.math.roundToInt

/** 不切换 Activity 的原生剪贴板悬浮面板。 */
class EcoPasteOverlayPanel(
    private val context: Context,
    private val windowManager: WindowManager,
    private val onSessionChanged: (Long?) -> Unit,
) {
    companion object {
        private const val TAG = "EcoPasteOverlayPanel"
        private const val ITEM_LIMIT = 50
        private const val CLOUD_RECORD_PAGE_SIZE = 30
    }

    private enum class ItemFilter {
        ALL,
        FAVORITE,
        TEXT,
        IMAGE,
        FILES,
    }

    private data class OverlayItem(
        val id: String,
        val kind: String,
        val tag: String,
        val preview: String,
        val detail: String,
        val sourceAppName: String,
        val sourceAppIconPath: String,
        val sourceAppAccentStart: String,
        val sourceAppAccentEnd: String,
        val displayCreatedAt: String,
        val isFavorite: Boolean,
        val isPinned: Boolean,
        val sync: ItemSyncStatus,
    )

    private data class ItemSyncChannel(
        val state: String,
        val deliveredTargets: Int,
        val totalTargets: Int,
        val lastError: String,
    )

    private data class ItemSyncStatus(
        val lan: ItemSyncChannel,
        val cloud: ItemSyncChannel,
    )

    private data class OverlayPeerStatus(
        val deviceId: String,
        val deviceName: String,
        val platform: String,
        val state: String,
        val connectedAddress: String,
        val directAddresses: List<String>,
        val transport: String,
        val lastSeenAt: String,
        val lastError: String,
    )

    private data class OverlaySyncStatus(
        val lanState: String,
        val lanEnabled: Boolean,
        val cloudState: String,
        val cloudEnabled: Boolean,
        val cloudEndpointId: String,
        val cloudDirectAddresses: List<String>,
        val cloudRelayUrls: List<String>,
        val cloudConnectedAddress: String,
        val cloudTransport: String,
        val cloudServerVersion: String,
        val cloudError: String,
        val cloudLastSuccessAt: String,
        val pendingEvents: Int,
        val peers: List<OverlayPeerStatus>,
    )

    private data class CloudRecord(
        val eventId: String,
        val deviceName: String,
        val kind: String,
        val preview: String,
        val imagePath: String,
        val createdAt: String,
        val fileCount: Int,
        val totalSize: Long,
        val isSensitive: Boolean,
    )

    private var reconnectInProgress = false

    private val mainHandler = Handler(Looper.getMainLooper())
    private var panelView: View? = null
    private var outsideView: View? = null
    private var activeSessionId: Long? = null
    private var nextSessionId = 0L
    private var loadGeneration = 0
    private var activeFilter = ItemFilter.ALL
    private var loadedItems = emptyList<OverlayItem>()
    private var itemContainer: LinearLayout? = null
    private var filterContainer: LinearLayout? = null
    private var searchInput: EditText? = null
    private var searchMode = false
    private var searchRunnable: Runnable? = null
    private var syncDetailsContainer: LinearLayout? = null
    private var syncDetailsCard: ScrollView? = null
    private var syncDetailsScrim: View? = null
    private var lanStatusButton: ImageButton? = null
    private var cloudStatusButton: ImageButton? = null
    private var expandedSyncTarget: String? = null
    private var cloudConnectionDetailsOpen = false
    private var syncStatus: OverlaySyncStatus? = null
    private var showingCloudRecords = false
    private var cloudRecords = emptyList<CloudRecord>()
    private var cloudNextBeforeCursor: Long? = null
    private var cloudImagePreview: View? = null
    private var syncStatusGeneration = 0
    private var cloudLoadGeneration = 0
    private var itemLoadFuture: Future<*>? = null
    private var syncStatusFuture: Future<*>? = null
    private var cloudLoadFuture: Future<*>? = null
    private val queryExecutor = Executors.newSingleThreadExecutor()
    private val actionExecutor = Executors.newFixedThreadPool(2)
    private val heightPersistenceExecutor = Executors.newSingleThreadExecutor()

    fun show(heightPercent: Int) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && !Settings.canDrawOverlays(context)) {
            Log.w(TAG, "overlay permission is unavailable")
            return
        }

        removeCurrentPanel()

        activeFilter = ItemFilter.ALL
        loadedItems = emptyList()
        showingCloudRecords = false
        cloudRecords = emptyList()
        cloudNextBeforeCursor = null

        val bounds = displayBounds()
        val initialHeightPercent = heightPercent.coerceIn(30, 90)
        val panelHeight = (bounds.height() * initialHeightPercent / 100f).toInt()
        val outsideHeight = (bounds.height() - panelHeight).coerceAtLeast(1)
        val sessionId = ++nextSessionId
        val root = GestureDismissFrameLayout(context, sessionId, outsideOnly = false)
        val outside = GestureDismissFrameLayout(context, sessionId, outsideOnly = true)
        val content = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(0, 0, 0, dp(8))
            background = panelBackground()
            elevation = dp(18).toFloat()
        }
        root.addView(
            content,
            FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT,
            ),
        )

        content.addView(createDragHandle(initialHeightPercent, bounds.height()))
        content.addView(createHeader())

        syncDetailsScrim = View(context).apply {
            visibility = View.GONE
            isClickable = true
            elevation = dp(20).toFloat()
            setOnClickListener { closeSyncDetails() }
        }
        root.addView(
            syncDetailsScrim,
            FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT,
            ).apply {
                topMargin = dp(64)
            },
        )

        val detailsContent = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(12), dp(12), dp(12), dp(12))
        }
        syncDetailsContainer = detailsContent
        syncDetailsCard = ScrollView(context).apply {
            visibility = View.GONE
            isVerticalScrollBarEnabled = false
            clipToOutline = true
            elevation = dp(24).toFloat()
            background = borderedRoundedBackground(
                cardColor(),
                borderColor(),
                dp(12).toFloat(),
            )
            addView(
                detailsContent,
                FrameLayout.LayoutParams(
                    FrameLayout.LayoutParams.MATCH_PARENT,
                    FrameLayout.LayoutParams.WRAP_CONTENT,
                ),
            )
        }
        val detailsRightMargin = dp(52)
        val detailsWidth = minOf(dp(288), bounds.width() - detailsRightMargin - dp(12))
        root.addView(
            syncDetailsCard,
            FrameLayout.LayoutParams(
                detailsWidth,
                FrameLayout.LayoutParams.WRAP_CONTENT,
                Gravity.TOP or Gravity.END,
            ).apply {
                topMargin = dp(64)
                rightMargin = detailsRightMargin
                bottomMargin = dp(8)
            },
        )

        val filters = LinearLayout(context).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(dp(12), dp(2), dp(12), dp(6))
        }
        filterContainer = filters
        renderFilters()
        content.addView(
            HorizontalScrollView(context).apply {
                isHorizontalScrollBarEnabled = false
                clipToPadding = false
                addView(
                    filters,
                    FrameLayout.LayoutParams(
                        FrameLayout.LayoutParams.WRAP_CONTENT,
                        FrameLayout.LayoutParams.WRAP_CONTENT,
                    ),
                )
            },
            LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT,
            ),
        )

        val body = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(12), dp(2), dp(12), dp(12))
            addView(statusText(R.string.overlay_panel_loading))
        }
        itemContainer = body
        content.addView(
            ScrollView(context).apply {
                isFillViewport = true
                isVerticalScrollBarEnabled = false
                clipToPadding = false
                addView(
                    body,
                    FrameLayout.LayoutParams(
                        FrameLayout.LayoutParams.MATCH_PARENT,
                        FrameLayout.LayoutParams.WRAP_CONTENT,
                    ),
                )
            },
            LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 0, 1f),
        )

        var outsideAdded = false
        var panelAdded = false
        try {
            windowManager.addView(outside, createLayoutParams(outsideHeight, Gravity.TOP))
            outsideAdded = true
            windowManager.addView(root, createLayoutParams(panelHeight, Gravity.BOTTOM))
            panelAdded = true
            panelView = root
            outsideView = outside
            activeSessionId = sessionId
            onSessionChanged(sessionId)
            EcoPasteBridge.setSyncStatusChangedListener { requestSyncStatus() }
            EcoPasteBridge.setClipboardDataChangedListener { requestVisibleItems() }
            installBottomSystemInset(root, content)
            installSystemGestureExclusion(outside, preserveBottomSystemArea = false)
            installSystemGestureExclusion(root, preserveBottomSystemArea = true)
        } catch (error: Exception) {
            Log.e(TAG, "show overlay panel failed: ${error.message}", error)
            if (panelAdded) removeViewImmediately(root)
            if (outsideAdded) removeViewImmediately(outside)
            clearPanelState()
            return
        }

        requestItems("")
        requestSyncStatus()
    }

    fun hide(sessionId: Long? = null) {
        removeCurrentPanel(sessionId)
    }

    /** Releases background workers owned by the long-lived overlay service. */
    fun destroy() {
        removeCurrentPanel()
        queryExecutor.shutdownNow()
        actionExecutor.shutdownNow()
        heightPersistenceExecutor.shutdownNow()
    }

    /** 返回面板内上一层；返回 false 表示当前已在根层，可由服务收起面板。 */
    fun navigateBack(sessionId: Long?): Boolean {
        if (sessionId != null && activeSessionId != sessionId) return true
        if (cloudImagePreview != null) {
            closeCloudImagePreview()
            return true
        }
        if (expandedSyncTarget != null) {
            closeSyncDetails()
            return true
        }
        if (showingCloudRecords) {
            closeCloudRecords()
            return true
        }

        return false
    }

    /** 强制移除旧窗口，避免 View 对象存在但 Surface 已不可见时阻塞下一次唤起。 */
    private fun removeCurrentPanel(expectedSessionId: Long? = null) {
        if (expectedSessionId != null && activeSessionId != expectedSessionId) return
        loadGeneration += 1
        val currentPanel = panelView
        val currentOutside = outsideView
        if (currentPanel == null && currentOutside == null) return
        hideKeyboard()
        clearPanelState()
        onSessionChanged(null)
        currentPanel?.let { removeViewImmediately(it) }
        currentOutside?.let { removeViewImmediately(it) }
    }

    private fun removeViewImmediately(view: View) {
        try {
            windowManager.removeViewImmediate(view)
        } catch (error: Exception) {
            Log.w(TAG, "remove overlay panel failed: ${error.message}")
        }
    }

    private fun clearPanelState() {
        loadGeneration += 1
        syncStatusGeneration += 1
        cloudLoadGeneration += 1
        itemLoadFuture?.cancel(false)
        syncStatusFuture?.cancel(false)
        cloudLoadFuture?.cancel(false)
        searchRunnable?.let { mainHandler.removeCallbacks(it) }
        searchRunnable = null
        searchInput = null
        searchMode = false
        EcoPasteBridge.setSyncStatusChangedListener(null)
        EcoPasteBridge.setClipboardDataChangedListener(null)
        panelView = null
        outsideView = null
        activeSessionId = null
        itemContainer = null
        filterContainer = null
        syncDetailsContainer = null
        syncDetailsCard = null
        syncDetailsScrim = null
        lanStatusButton?.clearAnimation()
        cloudStatusButton?.clearAnimation()
        lanStatusButton = null
        cloudStatusButton = null
        expandedSyncTarget = null
        cloudConnectionDetailsOpen = false
        syncStatus = null
        cloudImagePreview = null
        loadedItems = emptyList()
    }

    private fun requestItems(keyword: String) {
        val generation = ++loadGeneration
        itemContainer?.apply {
            removeAllViews()
            addView(statusText(R.string.overlay_panel_loading))
        }
        itemLoadFuture?.cancel(false)
        itemLoadFuture = queryExecutor.submit {
            val json = try {
                EcoPasteBridge.loadOverlayItemsJson(keyword.trim(), ITEM_LIMIT)
            } catch (error: Throwable) {
                Log.e(TAG, "load overlay items failed: ${error.message}", error)
                "[]"
            }
            mainHandler.post {
                if (generation == loadGeneration && panelView != null) {
                    loadedItems = parseItems(json)
                    renderItems()
                }
            }
        }
    }

    /** Reloads the current native view only after Rust has committed a clipboard change. */
    private fun requestVisibleItems() {
        if (panelView == null || showingCloudRecords) return
        requestItems(searchInput?.text?.toString().orEmpty())
    }

    private fun requestSyncStatus() {
        if (panelView == null) return
        val generation = ++syncStatusGeneration
        syncStatusFuture?.cancel(false)
        syncStatusFuture = queryExecutor.submit {
            val json = try {
                EcoPasteBridge.loadOverlaySyncStatusJson()
            } catch (error: Throwable) {
                Log.e(TAG, "load overlay sync status failed: ${error.message}", error)
                "{}"
            }
            mainHandler.post {
                if (panelView == null || generation != syncStatusGeneration) return@post
                syncStatus = parseSyncStatus(json)
                renderTopSyncStatus()
                renderSyncDetails()
            }
        }
    }

    private fun parseSyncStatus(json: String): OverlaySyncStatus {
        val root = try {
            JSONObject(json)
        } catch (error: Exception) {
            Log.e(TAG, "parse overlay sync status failed: ${error.message}", error)
            JSONObject()
        }
        val peersJson = root.optJSONArray("peers") ?: JSONArray()
        val peers = buildList {
            for (index in 0 until peersJson.length()) {
                val peer = peersJson.optJSONObject(index) ?: continue
                val directJson = peer.optJSONArray("directAddresses") ?: JSONArray()
                val addresses = buildList {
                    for (addressIndex in 0 until directJson.length()) {
                        directJson.optString(addressIndex).takeIf { it.isNotBlank() }?.let(::add)
                    }
                }
                add(
                    OverlayPeerStatus(
                        deviceId = peer.optString("deviceId"),
                        deviceName = peer.optString("deviceName", "EcoPaste"),
                        platform = peer.optString("platform"),
                        state = peer.optString("state", "idle"),
                        connectedAddress = optionalJsonString(peer, "connectedAddress"),
                        directAddresses = addresses,
                        transport = optionalJsonString(peer, "transport"),
                        lastSeenAt = optionalJsonString(peer, "lastSeenAt"),
                        lastError = optionalJsonString(peer, "lastError"),
                    ),
                )
            }
        }
        val lan = root.optJSONObject("lan") ?: JSONObject()
        val cloud = root.optJSONObject("cloud") ?: JSONObject()
        return OverlaySyncStatus(
            lanState = lan.optString("state", "disabled"),
            lanEnabled = root.optBoolean("lanEnabled"),
            cloudState = cloud.optString("state", "disabled"),
            cloudEnabled = root.optBoolean("cloudEnabled"),
            cloudEndpointId = root.optString("cloudEndpointId"),
            cloudDirectAddresses = jsonStrings(root.optJSONArray("cloudDirectAddresses")),
            cloudRelayUrls = jsonStrings(root.optJSONArray("cloudRelayUrls")),
            cloudConnectedAddress = optionalJsonString(root, "cloudConnectedAddress"),
            cloudTransport = optionalJsonString(root, "cloudTransport"),
            cloudServerVersion = optionalJsonString(root, "cloudServerVersion"),
            cloudError = optionalJsonString(cloud, "lastError"),
            cloudLastSuccessAt = optionalJsonString(cloud, "lastSuccessAt"),
            pendingEvents = root.optInt("pendingEvents"),
            peers = peers,
        )
    }

    private fun jsonStrings(values: JSONArray?): List<String> {
        val array = values ?: return emptyList()
        return buildList {
            for (index in 0 until array.length()) {
                array.optString(index).takeIf { it.isNotBlank() }?.let(::add)
            }
        }
    }

    private fun optionalJsonString(value: JSONObject, key: String): String {
        return value.optString(key).takeUnless { it == "null" }.orEmpty()
    }

    private fun renderTopSyncStatus() {
        val status = syncStatus
        renderTopSyncButton(
            lanStatusButton,
            "lan",
            status?.lanState,
            "局域网同步",
        )
        renderTopSyncButton(
            cloudStatusButton,
            "cloud",
            status?.cloudState,
            "云端同步",
        )
    }

    private fun toggleSyncDetails(target: String) {
        expandedSyncTarget = if (expandedSyncTarget == target) null else target
        if (expandedSyncTarget != "cloud") cloudConnectionDetailsOpen = false
        renderTopSyncStatus()
        renderSyncDetails()
    }

    private fun closeSyncDetails() {
        if (expandedSyncTarget == null) return
        expandedSyncTarget = null
        cloudConnectionDetailsOpen = false
        renderTopSyncStatus()
        renderSyncDetails()
    }

    private fun renderSyncDetails() {
        val container = syncDetailsContainer ?: return
        val card = syncDetailsCard ?: return
        val target = expandedSyncTarget
        if (target == null) {
            container.removeAllViews()
            card.visibility = View.GONE
            syncDetailsScrim?.visibility = View.GONE
            return
        }

        syncDetailsScrim?.apply {
            visibility = View.VISIBLE
            bringToFront()
        }
        card.visibility = View.VISIBLE
        card.bringToFront()
        container.removeAllViews()
        val status = syncStatus
        val state = if (target == "lan") status?.lanState else status?.cloudState
        container.addView(createSyncDetailsHeader(target, state, status))

        if (target == "lan") {
            renderLanSyncDetails(container, status)
        } else {
            renderCloudSyncDetails(container, status)
        }
        card.scrollTo(0, 0)
    }

    private fun createSyncDetailsHeader(
        target: String,
        state: String?,
        status: OverlaySyncStatus?,
    ): View {
        val iconRes = if (target == "lan") R.drawable.ic_sync_lan else R.drawable.ic_sync_cloud
        return LinearLayout(context).apply {
            gravity = Gravity.CENTER_VERTICAL
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, 0, 0, dp(10))
            addView(ImageView(context).apply {
                setImageResource(iconRes)
                imageTintList = ColorStateList.valueOf(syncStateColor(state))
                scaleType = ImageView.ScaleType.CENTER_INSIDE
            }, LinearLayout.LayoutParams(dp(20), dp(20)).apply {
                marginEnd = dp(8)
            })
            addView(TextView(context).apply {
                text = if (target == "lan") "局域网" else "云端"
                textSize = 14f
                typeface = Typeface.DEFAULT_BOLD
                setTextColor(primaryTextColor())
            })
            addView(TextView(context).apply {
                text = syncStateLabel(state)
                textSize = 11f
                setTextColor(syncStateColor(state))
            }, LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.WRAP_CONTENT,
                LinearLayout.LayoutParams.WRAP_CONTENT,
            ).apply {
                marginStart = dp(8)
            })

            addView(View(context), LinearLayout.LayoutParams(0, 1, 1f))
            if (target == "lan" && status?.lanEnabled == true) {
                addView(reconnectButton("重新连接全部设备") { reconnectPeer(null) })
            }
        }
    }

    private fun renderLanSyncDetails(
        container: LinearLayout,
        status: OverlaySyncStatus?,
    ) {
        val peers = status?.peers.orEmpty()
        if (peers.isEmpty()) {
            container.addView(emptySyncDetails("暂无已配对设备"))
            return
        }

        peers.forEach { peer ->
            container.addView(
                createLanPeerCard(peer, status?.lanEnabled == true),
                LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                ).apply {
                    bottomMargin = dp(8)
                },
            )
        }
    }

    private fun createLanPeerCard(peer: OverlayPeerStatus, lanEnabled: Boolean): View {
        val isOnline = peer.state == "online"
        val address = if (isOnline) {
            peer.connectedAddress
        } else {
            peer.directAddresses.firstOrNull().orEmpty()
        }
        val transport = transportLabel(peer.transport)

        return LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(10), dp(9), dp(8), dp(9))
            background = roundedBackground(subtleFillColor(), dp(10).toFloat())

            addView(LinearLayout(context).apply {
                gravity = Gravity.CENTER_VERTICAL
                orientation = LinearLayout.HORIZONTAL
                addView(TextView(context).apply {
                    text = peer.deviceName
                    textSize = 12f
                    maxLines = 1
                    typeface = Typeface.DEFAULT_BOLD
                    setTextColor(primaryTextColor())
                }, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
                addView(TextView(context).apply {
                    text = syncStateLabel(peer.state)
                    textSize = 10f
                    setTextColor(syncStateColor(peer.state))
                })
                if (lanEnabled) {
                    addView(
                        reconnectButton("重新连接 ${peer.deviceName}") {
                            reconnectPeer(peer.deviceId)
                        },
                    )
                }
            })

            val platformAndTransport = listOf(peer.platform, transport)
                .filter { it.isNotBlank() }
                .joinToString(" · ")
            if (platformAndTransport.isNotBlank()) {
                addView(detailLine(platformAndTransport))
            }
            if (address.isNotBlank()) {
                addView(detailLine(address, monospace = true))
            }
            if (peer.lastSeenAt.isNotBlank()) {
                addView(detailLine("最近在线：${formatSyncTimestamp(peer.lastSeenAt)}"))
            }
            if (peer.lastError.isNotBlank()) {
                addView(errorDetails(peer.lastError))
            }
        }
    }

    private fun renderCloudSyncDetails(
        container: LinearLayout,
        status: OverlaySyncStatus?,
    ) {
        if (status?.cloudEnabled != true) {
            container.addView(emptySyncDetails("云端同步未启用"))
            return
        }

        container.addView(
            createInfoRow(
                "Hub 版本",
                status.cloudServerVersion.ifBlank { "版本未知" },
                emphasize = status.cloudServerVersion.isNotBlank(),
            ),
        )
        container.addView(
            createInfoRow(
                "连接方式",
                if (status.cloudConnectedAddress.isBlank()) {
                    "尚未连接"
                } else {
                    transportLabel(status.cloudTransport).ifBlank { "未知" }
                },
            ),
        )
        if (status.cloudConnectedAddress.isNotBlank()) {
            container.addView(
                createInfoRow(
                    "当前路径",
                    status.cloudConnectedAddress,
                    monospace = true,
                ),
            )
        }
        if (status.cloudLastSuccessAt.isNotBlank()) {
            container.addView(
                createInfoRow(
                    "最近成功",
                    formatSyncTimestamp(status.cloudLastSuccessAt),
                ),
            )
        }
        container.addView(
            createInfoRow(
                "待同步",
                if (status.pendingEvents > 0) "${status.pendingEvents} 条" else "无",
                warning = status.pendingEvents > 0,
            ),
        )

        if (status.cloudError.isNotBlank()) {
            container.addView(errorDetails(status.cloudError))
        }

        val hasConnectionDetails = status.cloudEndpointId.isNotBlank() ||
            status.cloudDirectAddresses.isNotEmpty() ||
            status.cloudRelayUrls.isNotEmpty()
        if (hasConnectionDetails) {
            container.addView(createConnectionDetailsToggle())
            if (cloudConnectionDetailsOpen) {
                container.addView(createCloudConnectionDetails(status))
            }
        }

        container.addView(
            createCloudRecordsButton(status.cloudEndpointId.isNotBlank()),
            LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                dp(36),
            ).apply {
                topMargin = dp(10)
            },
        )
    }

    private fun detailText(value: String, error: Boolean = false): TextView {
        return TextView(context).apply {
            text = value
            textSize = 11f
            setTextColor(if (error) syncStateColor("error") else secondaryTextColor())
            setPadding(dp(12), dp(3), dp(12), dp(3))
        }
    }

    private fun detailLine(value: String, monospace: Boolean = false): TextView {
        return TextView(context).apply {
            text = value
            textSize = if (monospace) 10f else 11f
            setTextColor(secondaryTextColor())
            setPadding(0, dp(3), 0, 0)
            if (monospace) typeface = Typeface.MONOSPACE
        }
    }

    private fun createInfoRow(
        label: String,
        value: String,
        emphasize: Boolean = false,
        monospace: Boolean = false,
        warning: Boolean = false,
    ): View {
        return LinearLayout(context).apply {
            gravity = Gravity.TOP
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, dp(5), 0, dp(5))
            addView(TextView(context).apply {
                text = label
                textSize = 11f
                setTextColor(secondaryTextColor())
            }, LinearLayout.LayoutParams(dp(68), LinearLayout.LayoutParams.WRAP_CONTENT))
            addView(TextView(context).apply {
                text = value
                textSize = if (monospace) 10f else 11f
                gravity = Gravity.END
                setTextColor(
                    when {
                        warning -> syncStateColor("degraded")
                        emphasize -> primaryTextColor()
                        else -> secondaryTextColor()
                    },
                )
                if (emphasize) typeface = Typeface.DEFAULT_BOLD
                if (monospace) typeface = Typeface.MONOSPACE
            }, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
        }
    }

    private fun emptySyncDetails(value: String): View {
        return TextView(context).apply {
            text = value
            textSize = 11f
            gravity = Gravity.CENTER
            setTextColor(secondaryTextColor())
            setPadding(dp(8), dp(18), dp(8), dp(18))
            background = roundedBackground(subtleFillColor(), dp(10).toFloat())
        }
    }

    private fun errorDetails(value: String): View {
        return TextView(context).apply {
            text = value
            textSize = 10f
            setTextColor(syncStateColor("error"))
            setPadding(dp(8), dp(7), dp(8), dp(7))
            background = roundedBackground(errorFillColor(), dp(8).toFloat())
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT,
            ).apply {
                topMargin = dp(6)
            }
        }
    }

    private fun createConnectionDetailsToggle(): View {
        return LinearLayout(context).apply {
            gravity = Gravity.CENTER_VERTICAL
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, dp(8), 0, dp(4))
            isClickable = true
            contentDescription = if (cloudConnectionDetailsOpen) "收起连接详情" else "展开连接详情"
            addView(TextView(context).apply {
                text = "连接详情"
                textSize = 11f
                setTextColor(primaryTextColor())
            }, LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f))
            addView(ImageView(context).apply {
                setImageResource(R.drawable.ic_chevron_right)
                imageTintList = ColorStateList.valueOf(secondaryTextColor())
                rotation = if (cloudConnectionDetailsOpen) 90f else 0f
            }, LinearLayout.LayoutParams(dp(16), dp(16)))
            setOnClickListener {
                cloudConnectionDetailsOpen = !cloudConnectionDetailsOpen
                renderSyncDetails()
            }
        }
    }

    private fun createCloudConnectionDetails(status: OverlaySyncStatus): View {
        return LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(9), dp(8), dp(9), dp(8))
            background = roundedBackground(subtleFillColor(), dp(9).toFloat())

            if (status.cloudEndpointId.isNotBlank()) {
                addView(technicalDetail("Endpoint ID", status.cloudEndpointId))
            }
            status.cloudDirectAddresses.forEach { address ->
                addView(technicalDetail("直连地址", address))
            }
            status.cloudRelayUrls.forEach { address ->
                addView(technicalDetail("Relay", address))
            }
        }
    }

    private fun technicalDetail(label: String, value: String): View {
        return LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            addView(TextView(context).apply {
                text = label
                textSize = 9f
                setTextColor(tertiaryTextColor())
            })
            addView(TextView(context).apply {
                text = value
                textSize = 9f
                typeface = Typeface.MONOSPACE
                setTextColor(secondaryTextColor())
            })
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT,
            ).apply {
                bottomMargin = dp(6)
            }
        }
    }

    private fun createCloudRecordsButton(enabled: Boolean): View {
        return LinearLayout(context).apply {
            gravity = Gravity.CENTER
            orientation = LinearLayout.HORIZONTAL
            isEnabled = enabled
            alpha = if (enabled) 1f else 0.45f
            background = borderedRoundedBackground(
                subtleFillColor(),
                borderColor(),
                dp(9).toFloat(),
            )
            addView(ImageView(context).apply {
                setImageResource(R.drawable.ic_cloud_download)
                imageTintList = ColorStateList.valueOf(Color.rgb(22, 119, 255))
                scaleType = ImageView.ScaleType.CENTER_INSIDE
            }, LinearLayout.LayoutParams(dp(16), dp(16)).apply {
                marginEnd = dp(6)
            })
            addView(TextView(context).apply {
                text = "查看云端记录"
                textSize = 11f
                typeface = Typeface.DEFAULT_BOLD
                setTextColor(Color.rgb(22, 119, 255))
            })
            setOnClickListener {
                if (enabled) {
                    closeSyncDetails()
                    requestCloudRecords()
                }
            }
        }
    }

    private fun reconnectButton(label: String, reconnect: () -> Unit): ImageButton {
        return ImageButton(context).apply {
            setImageResource(R.drawable.ic_sync_refresh)
            imageTintList = ColorStateList.valueOf(secondaryTextColor())
            scaleType = ImageView.ScaleType.CENTER_INSIDE
            background = null
            contentDescription = label
            setPadding(dp(7), dp(7), dp(7), dp(7))
            setOnClickListener { reconnect() }
            layoutParams = LinearLayout.LayoutParams(dp(32), dp(32))
        }
    }

    private fun transportLabel(transport: String): String {
        return when (transport) {
            "direct" -> "直连"
            "relay" -> "中继"
            else -> ""
        }
    }

    private fun reconnectPeer(deviceId: String?) {
        if (reconnectInProgress) return
        reconnectInProgress = true
        Toast.makeText(context, "正在重新连接", Toast.LENGTH_SHORT).show()
        actionExecutor.submit {
            val succeeded = try {
                EcoPasteBridge.reconnectOverlayPeer(deviceId.orEmpty())
            } catch (error: Throwable) {
                Log.w(TAG, "reconnect overlay peer failed: ${error.message}")
                false
            }
            mainHandler.post {
                reconnectInProgress = false
                if (panelView == null) return@post
                requestSyncStatus()
                if (!succeeded) {
                    Toast.makeText(context, "重新连接失败", Toast.LENGTH_SHORT).show()
                }
            }
        }
    }

    private fun requestCloudRecords(append: Boolean = false) {
        if (panelView == null) return
        if (append && cloudNextBeforeCursor == null) return
        showingCloudRecords = true
        filterContainer?.visibility = View.GONE
        if (!append) {
            cloudRecords = emptyList()
            cloudNextBeforeCursor = null
            itemContainer?.apply {
                removeAllViews()
                addView(statusText(R.string.overlay_panel_loading))
            }
        }
        val generation = ++cloudLoadGeneration
        cloudLoadFuture?.cancel(false)
        cloudLoadFuture = queryExecutor.submit {
            val json = try {
                EcoPasteBridge.loadOverlayCloudRecordsJson(
                    if (append) cloudNextBeforeCursor ?: -1L else -1L,
                    CLOUD_RECORD_PAGE_SIZE,
                )
            } catch (error: Throwable) {
                Log.e(TAG, "load overlay cloud records failed: ${error.message}", error)
                JSONObject().put("error", error.message.orEmpty()).toString()
            }
            mainHandler.post {
                if (
                    panelView == null ||
                    !showingCloudRecords ||
                    generation != cloudLoadGeneration
                ) return@post
                renderCloudRecords(json, append)
            }
        }
    }

    private fun renderCloudRecords(json: String, append: Boolean) {
        val container = itemContainer ?: return
        container.removeAllViews()
        container.addView(TextView(context).apply {
            text = "‹  云端剪贴板记录"
            textSize = 14f
            typeface = Typeface.DEFAULT_BOLD
            setTextColor(primaryTextColor())
            setPadding(dp(4), dp(6), dp(4), dp(12))
            setOnClickListener { closeCloudRecords() }
        })
        val root = try {
            JSONObject(json)
        } catch (error: Exception) {
            JSONObject().put("error", error.message.orEmpty())
        }
        val error = root.optString("error")
        if (error.isNotBlank()) {
            container.addView(detailText(error, error = true))
            return
        }
        val values = root.optJSONArray("records") ?: JSONArray()
        val pageRecords = buildList {
            for (index in 0 until values.length()) {
                val value = values.optJSONObject(index) ?: continue
                add(parseCloudRecord(value))
            }
        }
        cloudRecords = if (append) cloudRecords + pageRecords else pageRecords
        cloudNextBeforeCursor = if (root.isNull("nextBeforeCursor")) {
            null
        } else {
            root.optLong("nextBeforeCursor")
        }
        if (cloudRecords.isEmpty()) {
            container.addView(detailText("云端暂无剪贴板记录"))
            return
        }
        cloudRecords.forEach { record ->
            container.addView(createCloudRecordCard(record))
        }
        if (cloudNextBeforeCursor != null) {
            container.addView(TextView(context).apply {
                text = "加载更多"
                textSize = 12f
                gravity = Gravity.CENTER
                typeface = Typeface.DEFAULT_BOLD
                setTextColor(Color.rgb(0, 122, 255))
                setPadding(dp(12), dp(10), dp(12), dp(10))
                setOnClickListener { requestCloudRecords(append = true) }
            })
        }
    }

    private fun parseCloudRecord(value: JSONObject): CloudRecord {
        return CloudRecord(
            eventId = value.optString("eventId"),
            deviceName = value.optString("deviceName", "EcoPaste"),
            kind = value.optString("kind", "text"),
            preview = value.optString("preview"),
            imagePath = value.optString("imagePath"),
            createdAt = value.optString("createdAt").replace('T', ' ').take(16),
            fileCount = value.optInt("fileCount"),
            totalSize = value.optLong("totalSize"),
            isSensitive = value.optBoolean("isSensitive"),
        )
    }

    private fun createCloudRecordCard(record: CloudRecord): View {
        return LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(12), dp(10), dp(12), dp(10))
            background = roundedBackground(cardColor(), dp(12).toFloat())
            addView(TextView(context).apply {
                text = buildString {
                    append(record.deviceName)
                    if (record.isSensitive) append("  ·  敏感")
                }
                textSize = 13f
                typeface = Typeface.DEFAULT_BOLD
                setTextColor(primaryTextColor())
            })
            addView(TextView(context).apply {
                text = buildString {
                    append(record.createdAt)
                    if (record.fileCount > 0) append(" · ${record.fileCount} 个文件")
                    if (record.totalSize > 0) append(" · ${formatBytes(record.totalSize)}")
                }
                textSize = 10f
                setTextColor(tertiaryTextColor())
                setPadding(0, dp(3), 0, dp(6))
            })
            if (record.kind == "image" && record.imagePath.isNotBlank()) {
                decodeScaledBitmap(record.imagePath, dp(640))?.let { bitmap ->
                    addView(ImageView(context).apply {
                        adjustViewBounds = true
                        maxHeight = dp(220)
                        scaleType = ImageView.ScaleType.CENTER_INSIDE
                        setImageBitmap(bitmap)
                        contentDescription = record.preview.ifBlank { "云端图片" }
                        setOnClickListener { showCloudImagePreview(record.imagePath) }
                    }, LinearLayout.LayoutParams(
                        LinearLayout.LayoutParams.MATCH_PARENT,
                        LinearLayout.LayoutParams.WRAP_CONTENT,
                    ))
                }
            }
            addView(TextView(context).apply {
                text = record.preview.ifBlank {
                    when (record.kind) {
                        "image" -> "图片"
                        "files" -> "文件"
                        else -> "空文本"
                    }
                }
                textSize = 13f
                setTextColor(primaryTextColor())
            })
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT,
            ).apply {
                bottomMargin = dp(8)
            }
        }
    }

    private fun closeCloudRecords() {
        showingCloudRecords = false
        cloudLoadGeneration += 1
        cloudLoadFuture?.cancel(false)
        filterContainer?.visibility = View.VISIBLE
        renderItems()
    }

    /** 在当前悬浮面板内展示图片大图，避免跳转 Activity 或退出当前层级。 */
    private fun showCloudImagePreview(path: String) {
        val root = panelView as? FrameLayout ?: return
        val bitmap = decodeScaledBitmap(path, displayBounds().width().coerceAtLeast(dp(1080)))
            ?: return
        closeCloudImagePreview()
        val preview = FrameLayout(context).apply {
            isClickable = true
            setBackgroundColor(Color.argb(235, 0, 0, 0))
            setOnClickListener { closeCloudImagePreview() }
            addView(ImageView(context).apply {
                scaleType = ImageView.ScaleType.FIT_CENTER
                setImageBitmap(bitmap)
                contentDescription = "关闭云端图片预览"
            }, FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT,
            ))
        }
        root.addView(preview, FrameLayout.LayoutParams(
            FrameLayout.LayoutParams.MATCH_PARENT,
            FrameLayout.LayoutParams.MATCH_PARENT,
        ))
        cloudImagePreview = preview
    }

    private fun closeCloudImagePreview() {
        val preview = cloudImagePreview ?: return
        (preview.parent as? FrameLayout)?.removeView(preview)
        cloudImagePreview = null
    }

    /** 按显示尺寸采样图片，避免云端原图直接解码导致悬浮服务占用过多内存。 */
    private fun decodeScaledBitmap(path: String, maxDimension: Int): Bitmap? {
        val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeFile(path, bounds)
        if (bounds.outWidth <= 0 || bounds.outHeight <= 0) return null

        var sampleSize = 1
        while (
            bounds.outWidth / sampleSize > maxDimension ||
            bounds.outHeight / sampleSize > maxDimension
        ) {
            sampleSize *= 2
        }
        return BitmapFactory.decodeFile(path, BitmapFactory.Options().apply {
            inSampleSize = sampleSize
        })
    }

    /** 上沿拖动区域：向上拉长、向下缩短，松手后把百分比写回统一设置。 */
    private fun createDragHandle(initialHeightPercent: Int, displayHeight: Int): View {
        var startRawY = 0f
        var startHeight = 0
        var currentHeightPercent = initialHeightPercent

        return FrameLayout(context).apply {
            addView(
                View(context).apply {
                    background = roundedBackground(
                        if (isDarkMode()) Color.argb(64, 255, 255, 255) else Color.argb(42, 0, 0, 0),
                        dp(4).toFloat(),
                    )
                },
                FrameLayout.LayoutParams(dp(40), dp(4), Gravity.CENTER),
            )
            setOnTouchListener { view, event ->
                when (event.actionMasked) {
                    MotionEvent.ACTION_DOWN -> {
                        startRawY = event.rawY
                        startHeight = panelView?.layoutParams?.height
                            ?: (displayHeight * initialHeightPercent / 100f).toInt()
                        view.parent?.requestDisallowInterceptTouchEvent(true)
                    }
                    MotionEvent.ACTION_MOVE -> {
                        val nextHeight = startHeight + (startRawY - event.rawY).roundToInt()
                        resizePanel(nextHeight, displayHeight)
                    }
                    MotionEvent.ACTION_UP, MotionEvent.ACTION_CANCEL -> {
                        view.parent?.requestDisallowInterceptTouchEvent(false)
                        if (event.actionMasked == MotionEvent.ACTION_UP) {
                            val currentHeight = panelView?.layoutParams?.height ?: startHeight
                            val nextPercent = (currentHeight * 100f / displayHeight)
                                .roundToInt()
                                .coerceIn(30, 90)
                            resizePanel(
                                (displayHeight * nextPercent / 100f).roundToInt(),
                                displayHeight,
                            )
                            if (currentHeightPercent != nextPercent) {
                                currentHeightPercent = nextPercent
                                persistPanelHeightPercent(nextPercent)
                            }
                        }
                    }
                }
                true
            }
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                dp(20),
            )
        }
    }

    /** 同步更新上下两个覆盖窗口，避免调整过程中出现不可点击的空隙。 */
    private fun resizePanel(requestedHeight: Int, displayHeight: Int) {
        val panel = panelView ?: return
        val outside = outsideView ?: return
        val minHeight = (displayHeight * 0.3f).roundToInt()
        val maxHeight = (displayHeight * 0.9f).roundToInt()
        val panelHeight = requestedHeight.coerceIn(minHeight, maxHeight)
        val outsideHeight = (displayHeight - panelHeight).coerceAtLeast(1)

        try {
            val outsideParams = outside.layoutParams as WindowManager.LayoutParams
            outsideParams.height = outsideHeight
            windowManager.updateViewLayout(outside, outsideParams)

            val panelParams = panel.layoutParams as WindowManager.LayoutParams
            panelParams.height = panelHeight
            windowManager.updateViewLayout(panel, panelParams)
            installSystemGestureExclusion(outside, preserveBottomSystemArea = false)
            installSystemGestureExclusion(panel, preserveBottomSystemArea = true)
        } catch (error: Exception) {
            Log.w(TAG, "resize overlay panel failed: ${error.message}")
        }
    }

    /** Rust 设置落盘成功后再更新 SharedPreferences 镜像，确保两份状态一致。 */
    private fun persistPanelHeightPercent(nextPercent: Int) {
        heightPersistenceExecutor.execute {
            val persisted = try {
                EcoPasteBridge.persistOverlayPanelHeightPercent(nextPercent)
            } catch (error: Throwable) {
                Log.e(TAG, "persist overlay panel height failed: ${error.message}", error)
                false
            }
            if (persisted) {
                EcoPasteBridge.rememberGesturePopupHeightPercent(context, nextPercent)
            }
        }
    }

    private fun createHeader(): View {
        return LinearLayout(context).apply {
            gravity = Gravity.CENTER_VERTICAL
            orientation = LinearLayout.HORIZONTAL
            setPadding(dp(12), 0, dp(12), dp(8))

            addView(
                EditText(context).apply {
                    hint = "⌕  搜索剪贴板历史..."
                    textSize = 14f
                    maxLines = 1
                    isSingleLine = true
                    isFocusable = false
                    isFocusableInTouchMode = false
                    isCursorVisible = false
                    setPadding(dp(12), 0, dp(12), 0)
                    setTextColor(primaryTextColor())
                    setHintTextColor(tertiaryTextColor())
                    background = roundedBackground(cardColor(), dp(12).toFloat())
                    searchInput = this
                    setOnClickListener { enterSearchMode(this) }
                    addTextChangedListener(object : TextWatcher {
                        override fun beforeTextChanged(
                            value: CharSequence?,
                            start: Int,
                            count: Int,
                            after: Int,
                        ) = Unit

                        override fun onTextChanged(
                            value: CharSequence?,
                            start: Int,
                            before: Int,
                            count: Int,
                        ) {
                            if (!searchMode) return
                            searchRunnable?.let { mainHandler.removeCallbacks(it) }
                            val runnable = Runnable { requestItems(value?.toString().orEmpty()) }
                            searchRunnable = runnable
                            mainHandler.postDelayed(runnable, 220L)
                        }

                        override fun afterTextChanged(value: Editable?) = Unit
                    })
                },
                LinearLayout.LayoutParams(0, dp(36), 1f),
            )

            addView(
                createTopSyncButton("lan", R.drawable.ic_sync_lan),
                LinearLayout.LayoutParams(dp(36), dp(36)).apply {
                    marginStart = dp(8)
                },
            )

            addView(
                createTopSyncButton("cloud", R.drawable.ic_sync_cloud),
                LinearLayout.LayoutParams(dp(36), dp(36)).apply {
                    marginStart = dp(4)
                },
            )

            addView(TextView(context).apply {
                text = "⋮"
                textSize = 22f
                gravity = Gravity.CENTER
                setTextColor(primaryTextColor())
                background = roundedBackground(cardColor(), dp(12).toFloat())
                setOnClickListener { anchor ->
                    closeSyncDetails()
                    PopupMenu(context, anchor).apply {
                        menu.add("关闭悬浮窗")
                        setOnMenuItemClickListener {
                            hide()
                            true
                        }
                        show()
                    }
                }
            }, LinearLayout.LayoutParams(dp(36), dp(36)).apply {
                marginStart = dp(8)
            })
        }
    }

    private fun createTopSyncButton(target: String, iconRes: Int): ImageButton {
        return ImageButton(context).apply {
            setImageResource(iconRes)
            imageTintList = ColorStateList.valueOf(tertiaryTextColor())
            scaleType = ImageView.ScaleType.CENTER_INSIDE
            setPadding(dp(8), dp(8), dp(8), dp(8))
            background = null
            contentDescription = if (target == "lan") "局域网同步状态" else "云端同步状态"
            setOnClickListener { toggleSyncDetails(target) }
            if (target == "lan") {
                lanStatusButton = this
            } else {
                cloudStatusButton = this
            }
        }
    }

    private fun renderTopSyncButton(
        button: ImageButton?,
        target: String,
        state: String?,
        label: String,
    ) {
        button ?: return
        button.imageTintList = ColorStateList.valueOf(syncStateColor(state))
        button.contentDescription = "$label，${syncStateLabel(state)}"
        button.background = if (expandedSyncTarget == target) {
            roundedBackground(selectedFillColor(), dp(18).toFloat())
        } else {
            null
        }
        button.clearAnimation()
        if (state == "connecting") {
            button.startAnimation(AlphaAnimation(0.45f, 1f).apply {
                duration = 700L
                repeatCount = Animation.INFINITE
                repeatMode = Animation.REVERSE
            })
        }
    }

    private fun enterSearchMode(input: EditText) {
        if (searchMode) return
        closeSyncDetails()
        val current = panelView ?: return
        val params = current.layoutParams as? WindowManager.LayoutParams ?: return
        searchMode = true
        input.isFocusable = true
        input.isFocusableInTouchMode = true
        input.isCursorVisible = true
        mainHandler.post {
            if (!searchMode || panelView !== current) return@post
            params.flags = params.flags and WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE.inv()
            params.softInputMode = searchSoftInputMode()
            try {
                windowManager.updateViewLayout(current, params)
            } catch (error: Exception) {
                searchMode = false
                input.isFocusable = false
                input.isFocusableInTouchMode = false
                input.isCursorVisible = false
                Log.w(TAG, "enable overlay search focus failed: ${error.message}")
                return@post
            }

            input.requestFocus()
            awaitSearchWindowFocus(input, 0)
        }
    }

    /** 等悬浮窗真正取得输入焦点后再请求输入法，首次点击即可完成。 */
    private fun awaitSearchWindowFocus(input: EditText, attempt: Int) {
        if (!searchMode || panelView == null) return
        if (input.hasWindowFocus()) {
            showSearchKeyboard(input)
            return
        }
        if (attempt >= 12) {
            Log.w(TAG, "overlay search window did not obtain focus")
            return
        }
        input.postDelayed({ awaitSearchWindowFocus(input, attempt + 1) }, 40L)
    }

    private fun showSearchKeyboard(input: EditText) {
        if (!searchMode || panelView == null) return
        input.requestFocus()
        input.post {
            val keyboard = context.getSystemService(Context.INPUT_METHOD_SERVICE) as? InputMethodManager
            keyboard?.showSoftInput(input, InputMethodManager.SHOW_IMPLICIT)
        }
    }

    private fun hideKeyboard() {
        val input = searchInput ?: return
        val keyboard = context.getSystemService(Context.INPUT_METHOD_SERVICE) as? InputMethodManager
        keyboard?.hideSoftInputFromWindow(input.windowToken, 0)
        input.clearFocus()
    }

    private fun renderFilters() {
        val container = filterContainer ?: return
        container.removeAllViews()
        val filters = listOf(
            ItemFilter.ALL to "◷  全部",
            ItemFilter.FAVORITE to "★  收藏",
            ItemFilter.TEXT to "T  文本",
            ItemFilter.IMAGE to "▧  图片",
            ItemFilter.FILES to "▰  文件",
        )
        filters.forEach { (filter, label) ->
            container.addView(createFilterChip(filter, label))
        }
    }

    private fun createFilterChip(filter: ItemFilter, label: String): TextView {
        val selected = activeFilter == filter
        return TextView(context).apply {
            text = label
            textSize = 12f
            gravity = Gravity.CENTER
            typeface = Typeface.create(Typeface.DEFAULT, Typeface.BOLD)
            setPadding(dp(14), 0, dp(14), 0)
            setTextColor(if (selected) Color.WHITE else secondaryTextColor())
            background = roundedBackground(
                if (selected) Color.rgb(0, 122, 255) else cardColor(),
                dp(18).toFloat(),
            )
            setOnClickListener {
                if (activeFilter == filter) return@setOnClickListener
                activeFilter = filter
                renderFilters()
                renderItems()
            }
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.WRAP_CONTENT,
                dp(32),
            ).apply {
                marginEnd = dp(6)
            }
        }
    }

    private fun parseItems(json: String): List<OverlayItem> {
        val items = try {
            JSONArray(json)
        } catch (error: Exception) {
            Log.e(TAG, "parse overlay items failed: ${error.message}", error)
            JSONArray()
        }
        return buildList {
            for (index in 0 until items.length()) {
                val item = items.optJSONObject(index) ?: continue
                val sync = item.optJSONObject("sync") ?: JSONObject()
                add(
                    OverlayItem(
                        id = item.optString("id"),
                        kind = item.optString("kind", "text"),
                        tag = item.optString("tag", "文本"),
                        preview = item.optString("preview"),
                        detail = item.optString("detail"),
                        sourceAppName = item.optString("sourceAppName", "EcoPaste"),
                        sourceAppIconPath = item.optString("sourceAppIconPath"),
                        sourceAppAccentStart = item.optString("sourceAppAccentStart"),
                        sourceAppAccentEnd = item.optString("sourceAppAccentEnd"),
                        displayCreatedAt = item.optString("displayCreatedAt"),
                        isFavorite = item.optBoolean("isFavorite"),
                        isPinned = item.optBoolean("isPinned"),
                        sync = ItemSyncStatus(
                            lan = parseItemSyncChannel(sync.optJSONObject("lan")),
                            cloud = parseItemSyncChannel(sync.optJSONObject("cloud")),
                        ),
                    ),
                )
            }
        }
    }

    private fun parseItemSyncChannel(value: JSONObject?): ItemSyncChannel {
        return ItemSyncChannel(
            state = value?.optString("state", "idle") ?: "idle",
            deliveredTargets = value?.optInt("deliveredTargets") ?: 0,
            totalTargets = value?.optInt("totalTargets") ?: 0,
            lastError = value?.optString("lastError").orEmpty(),
        )
    }

    private fun renderItems() {
        if (showingCloudRecords) return
        val container = itemContainer ?: return
        container.removeAllViews()
        val visibleItems = loadedItems.filter { item ->
            when (activeFilter) {
                ItemFilter.ALL -> true
                ItemFilter.FAVORITE -> item.isFavorite
                ItemFilter.TEXT -> item.kind == "text"
                ItemFilter.IMAGE -> item.kind == "image"
                ItemFilter.FILES -> item.kind == "files"
            }
        }
        if (visibleItems.isEmpty()) {
            container.addView(statusText(R.string.overlay_panel_empty))
            return
        }
        visibleItems.forEach { item ->
            container.addView(createItemCard(item))
        }
    }

    private fun createItemCard(item: OverlayItem): View {
        return LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            clipToOutline = true
            background = roundedBackground(cardColor(), dp(16).toFloat())
            elevation = dp(2).toFloat()

            addView(createCardHeader(item))
            addView(TextView(context).apply {
                text = item.preview
                textSize = 15f
                maxLines = 4
                setTextColor(primaryTextColor())
                setPadding(dp(12), dp(10), dp(12), dp(6))
            }, LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1f,
            ))
            addView(createCardFooter(item))

            setOnClickListener { pasteItem(item.id) }
            setOnTouchListener { view, event ->
                when (event.actionMasked) {
                    MotionEvent.ACTION_DOWN -> view.alpha = 0.82f
                    MotionEvent.ACTION_UP, MotionEvent.ACTION_CANCEL -> view.alpha = 1f
                }
                false
            }
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                dp(118),
            ).apply {
                bottomMargin = dp(8)
            }
        }
    }

    private fun createCardHeader(item: OverlayItem): View {
        return LinearLayout(context).apply {
            gravity = Gravity.CENTER_VERTICAL
            orientation = LinearLayout.HORIZONTAL
            setPadding(dp(14), 0, dp(10), 0)
            background = headerBackground(item)

            addView(LinearLayout(context).apply {
                orientation = LinearLayout.VERTICAL
                gravity = Gravity.CENTER_VERTICAL
                addView(TextView(context).apply {
                    text = buildString {
                        append(item.tag)
                        if (item.isPinned) append("  · 置顶")
                        if (item.isFavorite) append("  ★")
                    }
                    textSize = 12f
                    typeface = Typeface.DEFAULT_BOLD
                    setTextColor(Color.WHITE)
                })
                addView(TextView(context).apply {
                    text = item.displayCreatedAt
                    textSize = 10f
                    setTextColor(Color.argb(220, 255, 255, 255))
                })
            }, LinearLayout.LayoutParams(0, dp(36), 1f))

            val iconBitmap = item.sourceAppIconPath
                .takeIf { it.isNotBlank() }
                ?.let { path -> BitmapFactory.decodeFile(path) }
            val iconView: View = if (iconBitmap != null) {
                ImageView(context).apply {
                    setImageBitmap(iconBitmap)
                    scaleType = ImageView.ScaleType.CENTER_INSIDE
                }
            } else {
                TextView(context).apply {
                    text = item.sourceAppName.trim().firstOrNull()?.uppercaseChar()?.toString() ?: "E"
                    textSize = 16f
                    gravity = Gravity.CENTER
                    typeface = Typeface.DEFAULT_BOLD
                    setTextColor(Color.WHITE)
                    background = roundedBackground(Color.argb(52, 255, 255, 255), dp(8).toFloat())
                }
            }
            addView(iconView, LinearLayout.LayoutParams(dp(32), dp(32)))
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                dp(36),
            )
        }
    }

    private fun createCardFooter(item: OverlayItem): View {
        return LinearLayout(context).apply {
            gravity = Gravity.CENTER_VERTICAL
            orientation = LinearLayout.HORIZONTAL
            setPadding(dp(12), 0, dp(12), dp(2))
            addView(TextView(context).apply {
                text = item.detail
                textSize = 11f
                maxLines = 1
                setTextColor(tertiaryTextColor())
            }, LinearLayout.LayoutParams(0, dp(28), 1f).apply {
                gravity = Gravity.CENTER_VERTICAL
            })
            addView(createItemSyncButton(item, "lan", R.drawable.ic_sync_lan))
            addView(createItemSyncButton(item, "cloud", R.drawable.ic_sync_cloud))
            addView(TextView(context).apply {
                text = "点击粘贴"
                textSize = 11f
                gravity = Gravity.CENTER_VERTICAL
                setTextColor(tertiaryTextColor())
            }, LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.WRAP_CONTENT,
                dp(28),
            ))
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                dp(28),
            )
        }
    }

    private fun createItemSyncButton(item: OverlayItem, target: String, iconRes: Int): ImageButton {
        val channel = if (target == "lan") item.sync.lan else item.sync.cloud
        val actionable = channel.state == "manual" || channel.state == "error"
        return ImageButton(context).apply {
            setImageResource(iconRes)
            imageTintList = ColorStateList.valueOf(itemSyncStateColor(channel.state))
            scaleType = ImageView.ScaleType.CENTER_INSIDE
            setPadding(dp(6), dp(6), dp(6), dp(6))
            background = null
            isEnabled = actionable
            contentDescription = itemSyncDescription(target, channel)
            setOnClickListener {
                if (actionable) synchronizeItem(item.id, target)
            }
            layoutParams = LinearLayout.LayoutParams(dp(28), dp(28))
            if (channel.state == "syncing") {
                startAnimation(AlphaAnimation(0.45f, 1f).apply {
                    duration = 700L
                    repeatCount = Animation.INFINITE
                    repeatMode = Animation.REVERSE
                })
            }
        }
    }

    private fun synchronizeItem(itemId: String, target: String) {
        actionExecutor.submit {
            val json = try {
                EcoPasteBridge.syncOverlayItemJson(itemId, target)
            } catch (error: Throwable) {
                Log.e(TAG, "sync overlay item failed: ${error.message}", error)
                JSONObject().put("error", error.message.orEmpty()).toString()
            }
            mainHandler.post {
                val result = try {
                    JSONObject(json)
                } catch (_: Exception) {
                    JSONObject().put("error", "同步失败")
                }
                val error = result.optString("error")
                if (error.isNotBlank()) {
                    Toast.makeText(context, error, Toast.LENGTH_SHORT).show()
                    return@post
                }
                val nextSync = ItemSyncStatus(
                    lan = parseItemSyncChannel(result.optJSONObject("lan")),
                    cloud = parseItemSyncChannel(result.optJSONObject("cloud")),
                )
                loadedItems = loadedItems.map { item ->
                    if (item.id == itemId) item.copy(sync = nextSync) else item
                }
                renderItems()
                requestSyncStatus()
                Toast.makeText(
                    context,
                    if (target == "lan") "已开始局域网同步" else "已开始云端同步",
                    Toast.LENGTH_SHORT,
                ).show()
            }
        }
    }

    private fun itemSyncDescription(target: String, channel: ItemSyncChannel): String {
        val name = if (target == "lan") "局域网" else "云端"
        return when (channel.state) {
            "success" -> if (target == "lan") {
                "$name：已投递到 ${channel.deliveredTargets}/${channel.totalTargets} 台设备"
            } else {
                "$name：已上传"
            }
            "syncing" -> "$name：同步中"
            "manual" -> "$name：需要手动同步，点击开始"
            "error" -> "$name：${channel.lastError.ifBlank { "同步失败" }}，点击重试"
            else -> if (target == "lan") "$name：当前无在线设备，已跳过" else "$name：本次未使用"
        }
    }

    private fun pasteItem(id: String) {
        val focusRestoreDelay = if (searchMode) 180L else 0L
        hide()
        mainHandler.postDelayed({
            actionExecutor.submit {
                val success = try {
                    EcoPasteBridge.pasteOverlayItem(id)
                } catch (error: Throwable) {
                    Log.e(TAG, "paste overlay item failed: ${error.message}", error)
                    false
                }
                if (!success) {
                    mainHandler.post {
                        Toast.makeText(
                            context,
                            R.string.overlay_panel_paste_failed,
                            Toast.LENGTH_SHORT,
                        ).show()
                    }
                }
            }
        }, focusRestoreDelay)
    }

    private fun statusText(textRes: Int): TextView {
        return TextView(context).apply {
            setText(textRes)
            textSize = 14f
            gravity = Gravity.CENTER
            setTextColor(secondaryTextColor())
            setPadding(dp(16), dp(48), dp(16), dp(48))
        }
    }

    private fun createLayoutParams(height: Int, gravityValue: Int): WindowManager.LayoutParams {
        val type = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY
        } else {
            @Suppress("DEPRECATION")
            WindowManager.LayoutParams.TYPE_PHONE
        }
        val flags = WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE or
            WindowManager.LayoutParams.FLAG_LAYOUT_IN_SCREEN or
            WindowManager.LayoutParams.FLAG_LAYOUT_NO_LIMITS
        return WindowManager.LayoutParams(
            WindowManager.LayoutParams.MATCH_PARENT,
            height,
            type,
            flags,
            PixelFormat.TRANSLUCENT,
        ).apply {
            gravity = gravityValue
            windowAnimations = 0
            softInputMode = WindowManager.LayoutParams.SOFT_INPUT_ADJUST_NOTHING
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                fitInsetsTypes = 0
                fitInsetsSides = 0
            }
        }
    }

    private fun installSystemGestureExclusion(root: View, preserveBottomSystemArea: Boolean) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) return
        root.post {
            if (root.width == 0 || root.height == 0) return@post
            val edgeWidth = dp(28)
            val systemGestureHeight = mandatorySystemGestureBottomInset(root.rootWindowInsets)
            val exclusionBottom = if (preserveBottomSystemArea) {
                (root.height - systemGestureHeight).coerceAtLeast(0)
            } else {
                root.height
            }
            if (exclusionBottom == 0) return@post
            root.systemGestureExclusionRects = listOf(
                Rect(0, 0, edgeWidth, exclusionBottom),
                Rect(root.width - edgeWidth, 0, root.width, exclusionBottom),
            )
        }
    }

    private fun mandatorySystemGestureBottomInset(insets: WindowInsets?): Int {
        if (insets == null) return dp(48)
        val bottom = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            insets.getInsets(WindowInsets.Type.mandatorySystemGestures()).bottom
        } else {
            @Suppress("DEPRECATION")
            insets.mandatorySystemGestureInsets.bottom
        }
        return bottom.coerceAtLeast(dp(48))
    }

    private fun searchSoftInputMode(): Int {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            return WindowManager.LayoutParams.SOFT_INPUT_ADJUST_NOTHING
        }
        @Suppress("DEPRECATION")
        return WindowManager.LayoutParams.SOFT_INPUT_ADJUST_RESIZE
    }

    /** 浮窗本身延伸到屏幕底部，但内容始终避开系统导航与手势区域。 */
    private fun installBottomSystemInset(root: View, content: View) {
        root.setOnApplyWindowInsetsListener { _, insets ->
            val bottomInset = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                insets.getInsets(WindowInsets.Type.systemBars() or WindowInsets.Type.ime()).bottom
            } else {
                @Suppress("DEPRECATION")
                insets.systemWindowInsetBottom
            }
            content.setPadding(0, 0, 0, bottomInset.coerceAtLeast(dp(8)))
            insets
        }
        root.requestApplyInsets()
    }

    private inner class GestureDismissFrameLayout(
        context: Context,
        private val sessionId: Long,
        private val outsideOnly: Boolean,
    ) : FrameLayout(context) {
        private var startX = 0f
        private var startY = 0f
        private var fromLeftEdge = false
        private var fromRightEdge = false
        private var fromHandle = false
        private var dismissRequested = false

        override fun dispatchTouchEvent(event: MotionEvent): Boolean {
            when (event.actionMasked) {
                MotionEvent.ACTION_OUTSIDE -> {
                    requestDismiss()
                    return true
                }
                MotionEvent.ACTION_DOWN -> {
                    startX = event.x
                    startY = event.y
                    val edgeWidth = dp(28).toFloat()
                    fromLeftEdge = startX <= edgeWidth
                    fromRightEdge = startX >= width - edgeWidth
                    if (outsideOnly) {
                        if (!fromLeftEdge && !fromRightEdge) {
                            requestDismiss()
                        }
                        return true
                    }
                    fromHandle = startY <= dp(28)
                    dismissRequested = false
                }
                MotionEvent.ACTION_MOVE -> {
                    val deltaX = event.x - startX
                    val deltaY = event.y - startY
                    val horizontalDismiss = !fromHandle && abs(deltaX) >= dp(52) &&
                        abs(deltaX) > abs(deltaY) * 1.2f &&
                        ((fromLeftEdge && deltaX > 0) || (fromRightEdge && deltaX < 0))
                    if (horizontalDismiss) {
                        requestDismiss()
                        return true
                    }
                    if (outsideOnly) return true
                }
                MotionEvent.ACTION_UP, MotionEvent.ACTION_CANCEL -> {
                    if (outsideOnly) {
                        requestDismiss()
                        fromLeftEdge = false
                        fromRightEdge = false
                        return true
                    }
                    fromLeftEdge = false
                    fromRightEdge = false
                    fromHandle = false
                }
            }
            return super.dispatchTouchEvent(event)
        }

        private fun requestDismiss() {
            if (dismissRequested) return
            dismissRequested = true
            mainHandler.post { hide(sessionId) }
        }
    }

    private fun displayBounds(): Rect {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            return windowManager.currentWindowMetrics.bounds
        }
        return Rect(
            0,
            0,
            context.resources.displayMetrics.widthPixels,
            context.resources.displayMetrics.heightPixels,
        )
    }

    private fun panelBackground(): GradientDrawable {
        val radius = dp(16).toFloat()
        return GradientDrawable().apply {
            shape = GradientDrawable.RECTANGLE
            setColor(panelColor())
            cornerRadii = floatArrayOf(radius, radius, radius, radius, 0f, 0f, 0f, 0f)
        }
    }

    private fun roundedBackground(color: Int, radius: Float): GradientDrawable {
        return GradientDrawable().apply {
            shape = GradientDrawable.RECTANGLE
            setColor(color)
            cornerRadius = radius
        }
    }

    private fun borderedRoundedBackground(
        color: Int,
        strokeColor: Int,
        radius: Float,
    ): GradientDrawable {
        return GradientDrawable().apply {
            shape = GradientDrawable.RECTANGLE
            setColor(color)
            setStroke(dp(1), strokeColor)
            cornerRadius = radius
        }
    }

    private fun headerColor(sourceAppName: String, kind: String): Int {
        val palette = when (kind) {
            "image" -> intArrayOf(Color.rgb(76, 175, 128), Color.rgb(53, 149, 141))
            "files" -> intArrayOf(Color.rgb(90, 126, 181), Color.rgb(97, 104, 177))
            else -> intArrayOf(
                Color.rgb(74, 144, 226),
                Color.rgb(88, 114, 196),
                Color.rgb(142, 104, 190),
                Color.rgb(213, 111, 93),
                Color.rgb(61, 156, 172),
            )
        }
        return palette[(sourceAppName.hashCode() and Int.MAX_VALUE) % palette.size]
    }

    /** Applies Rust-provided source colors and keeps the legacy palette as a safe fallback. */
    private fun headerBackground(item: OverlayItem): GradientDrawable {
        val start = runCatching { Color.parseColor(item.sourceAppAccentStart) }.getOrNull()
        val end = runCatching { Color.parseColor(item.sourceAppAccentEnd) }.getOrNull()
        if (start == null || end == null) {
            return roundedBackground(headerColor(item.sourceAppName, item.kind), 0f)
        }
        return GradientDrawable(
            GradientDrawable.Orientation.TL_BR,
            intArrayOf(start, end),
        )
    }

    private fun isDarkMode(): Boolean {
        val mode = context.resources.configuration.uiMode and Configuration.UI_MODE_NIGHT_MASK
        return mode == Configuration.UI_MODE_NIGHT_YES
    }

    private fun panelColor(): Int = if (isDarkMode()) Color.rgb(18, 18, 18) else Color.rgb(242, 242, 247)

    private fun cardColor(): Int = if (isDarkMode()) Color.rgb(38, 38, 40) else Color.WHITE

    private fun subtleFillColor(): Int =
        if (isDarkMode()) Color.rgb(48, 48, 50) else Color.rgb(247, 247, 249)

    private fun selectedFillColor(): Int =
        if (isDarkMode()) Color.rgb(54, 54, 57) else Color.rgb(232, 232, 237)

    private fun borderColor(): Int =
        if (isDarkMode()) Color.rgb(62, 62, 66) else Color.rgb(225, 225, 230)

    private fun errorFillColor(): Int =
        if (isDarkMode()) Color.rgb(62, 39, 40) else Color.rgb(255, 241, 240)

    private fun primaryTextColor(): Int = if (isDarkMode()) Color.rgb(245, 245, 247) else Color.rgb(30, 30, 30)

    private fun secondaryTextColor(): Int = if (isDarkMode()) Color.rgb(196, 196, 200) else Color.rgb(92, 92, 96)

    private fun tertiaryTextColor(): Int = Color.rgb(142, 142, 147)

    private fun syncStateColor(state: String?): Int {
        return when (state) {
            "online" -> Color.rgb(82, 196, 26)
            "connecting" -> Color.rgb(22, 119, 255)
            "degraded" -> Color.rgb(250, 173, 20)
            "error" -> Color.rgb(255, 77, 79)
            else -> tertiaryTextColor()
        }
    }

    private fun itemSyncStateColor(state: String): Int {
        return when (state) {
            "success" -> Color.rgb(82, 196, 26)
            "syncing" -> Color.rgb(22, 119, 255)
            "manual" -> Color.rgb(250, 173, 20)
            "error" -> Color.rgb(255, 77, 79)
            else -> tertiaryTextColor()
        }
    }

    private fun syncStateLabel(state: String?): String {
        return when (state) {
            "online" -> "在线"
            "connecting" -> "连接中"
            "degraded" -> "部分异常"
            "error" -> "异常"
            "disabled" -> "未启用"
            else -> "离线"
        }
    }

    private fun formatSyncTimestamp(value: String): String {
        val parsed = runCatching {
            SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }.parse(value.take(19))
        }.getOrNull() ?: return value
        return DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).format(parsed)
    }

    private fun formatBytes(bytes: Long): String {
        if (bytes < 1024) return "$bytes B"
        val units = arrayOf("KB", "MB", "GB", "TB")
        var size = bytes.toDouble() / 1024.0
        var unit = 0
        while (size >= 1024 && unit < units.lastIndex) {
            size /= 1024
            unit += 1
        }
        return if (size >= 10) "%.0f %s".format(size, units[unit]) else "%.1f %s".format(size, units[unit])
    }

    private fun dp(value: Int): Int = (value * context.resources.displayMetrics.density).toInt()
}
