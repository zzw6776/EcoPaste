package com.ayangweb.eco_paste

import android.content.Context
import android.content.res.ColorStateList
import android.content.res.Configuration
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
import android.view.WindowManager
import android.view.inputmethod.InputMethodManager
import android.widget.EditText
import android.widget.FrameLayout
import android.widget.HorizontalScrollView
import android.widget.ImageButton
import android.widget.LinearLayout
import android.widget.PopupMenu
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import org.json.JSONArray
import org.json.JSONObject
import kotlin.math.abs

/** 不切换 Activity 的原生剪贴板悬浮面板。 */
class EcoPasteOverlayPanel(
    private val context: Context,
    private val windowManager: WindowManager,
    private val onSessionChanged: (Long?) -> Unit,
) {
    companion object {
        private const val TAG = "EcoPasteOverlayPanel"
        private const val ITEM_LIMIT = 50
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
    )

    private data class OverlaySyncStatus(
        val lanState: String,
        val cloudState: String,
        val cloudEnabled: Boolean,
        val cloudEndpointId: String,
        val cloudDirectAddresses: List<String>,
        val cloudRelayUrls: List<String>,
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
    private var lanStatusButton: ImageButton? = null
    private var cloudStatusButton: ImageButton? = null
    private var expandedSyncTarget: String? = null
    private var syncStatus: OverlaySyncStatus? = null
    private var showingCloudRecords = false

    fun show(heightPercent: Int) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && !Settings.canDrawOverlays(context)) {
            Log.w(TAG, "overlay permission is unavailable")
            return
        }

        removeCurrentPanel()

        activeFilter = ItemFilter.ALL
        loadedItems = emptyList()
        showingCloudRecords = false

        val bounds = displayBounds()
        val panelHeight = (bounds.height() * heightPercent.coerceIn(30, 90) / 100f).toInt()
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

        content.addView(createDragHandle())
        content.addView(createHeader())
        syncDetailsContainer = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            visibility = View.GONE
            setPadding(dp(12), 0, dp(12), dp(8))
        }
        content.addView(syncDetailsContainer)

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
        searchRunnable?.let { mainHandler.removeCallbacks(it) }
        searchRunnable = null
        searchInput = null
        searchMode = false
        EcoPasteBridge.setSyncStatusChangedListener(null)
        panelView = null
        outsideView = null
        activeSessionId = null
        itemContainer = null
        filterContainer = null
        syncDetailsContainer = null
        lanStatusButton = null
        cloudStatusButton = null
        expandedSyncTarget = null
        syncStatus = null
        loadedItems = emptyList()
    }

    private fun requestItems(keyword: String) {
        val generation = ++loadGeneration
        itemContainer?.apply {
            removeAllViews()
            addView(statusText(R.string.overlay_panel_loading))
        }
        Thread({
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
        }, "ecopaste-overlay-load").start()
    }

    private fun requestSyncStatus() {
        if (panelView == null) return
        Thread({
            val json = try {
                EcoPasteBridge.loadOverlaySyncStatusJson()
            } catch (error: Throwable) {
                Log.e(TAG, "load overlay sync status failed: ${error.message}", error)
                "{}"
            }
            mainHandler.post {
                if (panelView == null) return@post
                syncStatus = parseSyncStatus(json)
                renderTopSyncStatus()
                renderSyncDetails()
            }
        }, "ecopaste-overlay-sync-status").start()
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
                        connectedAddress = peer.optString("connectedAddress"),
                        directAddresses = addresses,
                        transport = peer.optString("transport"),
                    ),
                )
            }
        }
        val lan = root.optJSONObject("lan") ?: JSONObject()
        val cloud = root.optJSONObject("cloud") ?: JSONObject()
        return OverlaySyncStatus(
            lanState = lan.optString("state", "disabled"),
            cloudState = cloud.optString("state", "disabled"),
            cloudEnabled = root.optBoolean("cloudEnabled"),
            cloudEndpointId = root.optString("cloudEndpointId"),
            cloudDirectAddresses = jsonStrings(root.optJSONArray("cloudDirectAddresses")),
            cloudRelayUrls = jsonStrings(root.optJSONArray("cloudRelayUrls")),
            cloudError = cloud.optString("lastError"),
            cloudLastSuccessAt = cloud.optString("lastSuccessAt"),
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

    private fun renderTopSyncStatus() {
        val status = syncStatus
        lanStatusButton?.imageTintList = ColorStateList.valueOf(syncStateColor(status?.lanState))
        cloudStatusButton?.imageTintList = ColorStateList.valueOf(syncStateColor(status?.cloudState))
    }

    private fun toggleSyncDetails(target: String) {
        expandedSyncTarget = if (expandedSyncTarget == target) null else target
        renderSyncDetails()
    }

    private fun renderSyncDetails() {
        val container = syncDetailsContainer ?: return
        val target = expandedSyncTarget
        if (target == null) {
            container.visibility = View.GONE
            container.removeAllViews()
            return
        }
        container.visibility = View.VISIBLE
        container.removeAllViews()
        val status = syncStatus
        val state = if (target == "lan") status?.lanState else status?.cloudState
        container.addView(LinearLayout(context).apply {
            gravity = Gravity.CENTER_VERTICAL
            orientation = LinearLayout.HORIZONTAL
            setPadding(dp(12), dp(6), dp(4), dp(2))
            addView(TextView(context).apply {
                text = "${if (target == "lan") "局域网" else "云端"} · ${syncStateLabel(state)}"
                textSize = 13f
                typeface = Typeface.DEFAULT_BOLD
                setTextColor(syncStateColor(state))
            }, LinearLayout.LayoutParams(0, dp(36), 1f).apply {
                gravity = Gravity.CENTER_VERTICAL
            })
            if (target == "lan") {
                addView(reconnectButton("重新连接全部设备") { reconnectPeer(null) })
            }
        })
        if (target == "lan") {
            if (status?.peers.isNullOrEmpty()) {
                container.addView(detailText("暂无已配对设备"))
            } else {
                status?.peers?.forEach { peer ->
                    val address = peer.connectedAddress.ifBlank {
                        peer.directAddresses.firstOrNull().orEmpty()
                    }
                    val route = when (peer.transport) {
                        "direct" -> "直连"
                        "relay" -> "中继"
                        else -> ""
                    }
                    container.addView(LinearLayout(context).apply {
                        gravity = Gravity.CENTER_VERTICAL
                        orientation = LinearLayout.HORIZONTAL
                        addView(
                            detailText(
                                buildString {
                                    append(peer.deviceName)
                                    append(" · ")
                                    append(syncStateLabel(peer.state))
                                    if (peer.platform.isNotBlank()) append(" · ${peer.platform}")
                                    if (route.isNotBlank()) append(" · $route")
                                    if (address.isNotBlank()) append("\n$address")
                                },
                            ),
                            LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f),
                        )
                        addView(
                            reconnectButton("重新连接 ${peer.deviceName}") {
                                reconnectPeer(peer.deviceId)
                            },
                        )
                    })
                }
            }
        } else {
            if (status?.cloudEnabled != true) {
                container.addView(detailText("云端同步未启用"))
            } else {
                if (status.cloudEndpointId.isNotBlank()) {
                    container.addView(detailText(status.cloudEndpointId))
                }
                (status.cloudDirectAddresses + status.cloudRelayUrls).forEach { address ->
                    container.addView(detailText(address))
                }
                if (status.cloudError.isNotBlank()) {
                    container.addView(detailText(status.cloudError, error = true))
                }
                if (status.cloudLastSuccessAt.isNotBlank()) {
                    container.addView(detailText("最近成功：${status.cloudLastSuccessAt}"))
                }
                if (status.pendingEvents > 0) {
                    container.addView(detailText("${status.pendingEvents} 条事件等待上传"))
                }
            }
            if (status?.cloudEnabled == true && status.cloudEndpointId.isNotBlank()) {
                container.addView(TextView(context).apply {
                    text = "☁  查看云端记录"
                    textSize = 12f
                    gravity = Gravity.CENTER
                    typeface = Typeface.DEFAULT_BOLD
                    setTextColor(Color.rgb(0, 122, 255))
                    setPadding(dp(12), dp(8), dp(12), dp(8))
                    setOnClickListener { requestCloudRecords() }
                })
            }
        }
        container.background = roundedBackground(cardColor(), dp(12).toFloat())
    }

    private fun detailText(value: String, error: Boolean = false): TextView {
        return TextView(context).apply {
            text = value
            textSize = 11f
            setTextColor(if (error) Color.rgb(255, 69, 58) else secondaryTextColor())
            setPadding(dp(12), dp(3), dp(12), dp(3))
        }
    }

    private fun reconnectButton(label: String, reconnect: () -> Unit): ImageButton {
        return ImageButton(context).apply {
            setImageResource(android.R.drawable.ic_popup_sync)
            imageTintList = ColorStateList.valueOf(secondaryTextColor())
            background = null
            contentDescription = label
            setPadding(dp(8), dp(8), dp(8), dp(8))
            setOnClickListener { reconnect() }
            layoutParams = LinearLayout.LayoutParams(dp(36), dp(36))
        }
    }

    private fun reconnectPeer(deviceId: String?) {
        if (reconnectInProgress) return
        reconnectInProgress = true
        Toast.makeText(context, "正在重新连接", Toast.LENGTH_SHORT).show()
        Thread({
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
        }, "ecopaste-overlay-reconnect").start()
    }

    private fun requestCloudRecords() {
        if (panelView == null) return
        showingCloudRecords = true
        filterContainer?.visibility = View.GONE
        itemContainer?.apply {
            removeAllViews()
            addView(statusText(R.string.overlay_panel_loading))
        }
        Thread({
            val json = try {
                EcoPasteBridge.loadOverlayCloudRecordsJson()
            } catch (error: Throwable) {
                Log.e(TAG, "load overlay cloud records failed: ${error.message}", error)
                JSONObject().put("error", error.message.orEmpty()).toString()
            }
            mainHandler.post {
                if (panelView == null || !showingCloudRecords) return@post
                renderCloudRecords(json)
            }
        }, "ecopaste-overlay-cloud-records").start()
    }

    private fun renderCloudRecords(json: String) {
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
        if (values.length() == 0) {
            container.addView(detailText("云端暂无剪贴板记录"))
            return
        }
        for (index in 0 until values.length()) {
            val value = values.optJSONObject(index) ?: continue
            container.addView(createCloudRecordCard(parseCloudRecord(value)))
        }
    }

    private fun parseCloudRecord(value: JSONObject): CloudRecord {
        return CloudRecord(
            eventId = value.optString("eventId"),
            deviceName = value.optString("deviceName", "EcoPaste"),
            kind = value.optString("kind", "text"),
            preview = value.optString("preview"),
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
            addView(TextView(context).apply {
                text = if (record.isSensitive) "敏感内容已隐藏" else record.preview.ifBlank {
                    when (record.kind) {
                        "image" -> "图片"
                        "files" -> "文件"
                        else -> "空文本"
                    }
                }
                textSize = 13f
                setTextColor(primaryTextColor())
                maxLines = 5
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
        filterContainer?.visibility = View.VISIBLE
        renderItems()
    }

    private fun createDragHandle(): View {
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
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                dp(20),
            )
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

            lanStatusButton = createTopSyncButton("lan", R.drawable.ic_sync_lan)
            addView(lanStatusButton, LinearLayout.LayoutParams(dp(36), dp(36)).apply {
                marginStart = dp(8)
            })

            cloudStatusButton = createTopSyncButton("cloud", R.drawable.ic_sync_cloud)
            addView(cloudStatusButton, LinearLayout.LayoutParams(dp(36), dp(36)).apply {
                marginStart = dp(4)
            })

            addView(TextView(context).apply {
                text = "⋮"
                textSize = 22f
                gravity = Gravity.CENTER
                setTextColor(primaryTextColor())
                background = roundedBackground(cardColor(), dp(12).toFloat())
                setOnClickListener { anchor ->
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
            scaleType = android.widget.ImageView.ScaleType.CENTER
            setPadding(dp(8), dp(8), dp(8), dp(8))
            background = roundedBackground(cardColor(), dp(12).toFloat())
            contentDescription = if (target == "lan") "局域网同步状态" else "云端同步状态"
            setOnClickListener { toggleSyncDetails(target) }
        }
    }

    private fun enterSearchMode(input: EditText) {
        if (searchMode) return
        val current = panelView ?: return
        val params = current.layoutParams as? WindowManager.LayoutParams ?: return
        searchMode = true
        input.isFocusable = true
        input.isFocusableInTouchMode = true
        input.isCursorVisible = true
        mainHandler.post {
            if (!searchMode || panelView !== current) return@post
            params.flags = params.flags and WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE.inv()
            params.softInputMode = WindowManager.LayoutParams.SOFT_INPUT_ADJUST_RESIZE
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
            setBackgroundColor(headerColor(item.sourceAppName, item.kind))

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

            addView(TextView(context).apply {
                text = item.sourceAppName.trim().firstOrNull()?.uppercaseChar()?.toString() ?: "E"
                textSize = 14f
                gravity = Gravity.CENTER
                typeface = Typeface.DEFAULT_BOLD
                setTextColor(Color.WHITE)
                background = roundedBackground(Color.argb(52, 255, 255, 255), dp(8).toFloat())
            }, LinearLayout.LayoutParams(dp(28), dp(28)))
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
            scaleType = android.widget.ImageView.ScaleType.CENTER
            setPadding(dp(6), dp(6), dp(6), dp(6))
            background = null
            isEnabled = actionable
            alpha = if (actionable) 1f else 0.82f
            contentDescription = itemSyncDescription(target, channel)
            setOnClickListener {
                if (actionable) synchronizeItem(item.id, target)
            }
        }.also {
            it.layoutParams = LinearLayout.LayoutParams(dp(28), dp(28))
        }
    }

    private fun synchronizeItem(itemId: String, target: String) {
        Thread({
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
        }, "ecopaste-overlay-sync-item").start()
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
            Thread({
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
            }, "ecopaste-overlay-paste").start()
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
            val systemGestureHeight = root.rootWindowInsets
                ?.mandatorySystemGestureInsets
                ?.bottom
                ?.coerceAtLeast(dp(48))
                ?: dp(48)
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
                    val horizontalDismiss = abs(deltaX) >= dp(52) &&
                        abs(deltaX) > abs(deltaY) * 1.2f &&
                        ((fromLeftEdge && deltaX > 0) || (fromRightEdge && deltaX < 0))
                    val verticalDismiss = fromHandle && deltaY >= dp(64) && abs(deltaY) > abs(deltaX)
                    if (horizontalDismiss || verticalDismiss) {
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

    private fun isDarkMode(): Boolean {
        val mode = context.resources.configuration.uiMode and Configuration.UI_MODE_NIGHT_MASK
        return mode == Configuration.UI_MODE_NIGHT_YES
    }

    private fun panelColor(): Int = if (isDarkMode()) Color.rgb(18, 18, 18) else Color.rgb(242, 242, 247)

    private fun cardColor(): Int = if (isDarkMode()) Color.rgb(38, 38, 40) else Color.WHITE

    private fun primaryTextColor(): Int = if (isDarkMode()) Color.rgb(245, 245, 247) else Color.rgb(30, 30, 30)

    private fun secondaryTextColor(): Int = if (isDarkMode()) Color.rgb(196, 196, 200) else Color.rgb(92, 92, 96)

    private fun tertiaryTextColor(): Int = Color.rgb(142, 142, 147)

    private fun syncStateColor(state: String?): Int {
        return when (state) {
            "online" -> Color.rgb(52, 199, 89)
            "connecting" -> Color.rgb(0, 122, 255)
            "error" -> Color.rgb(255, 69, 58)
            else -> tertiaryTextColor()
        }
    }

    private fun itemSyncStateColor(state: String): Int {
        return when (state) {
            "success" -> Color.rgb(52, 199, 89)
            "syncing" -> Color.rgb(0, 122, 255)
            "manual" -> Color.rgb(255, 159, 10)
            "error" -> Color.rgb(255, 69, 58)
            else -> tertiaryTextColor()
        }
    }

    private fun syncStateLabel(state: String?): String {
        return when (state) {
            "online" -> "在线"
            "connecting" -> "连接中"
            "error" -> "异常"
            "disabled" -> "未启用"
            else -> "离线"
        }
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
