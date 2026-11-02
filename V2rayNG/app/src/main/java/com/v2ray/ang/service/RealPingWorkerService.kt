package com.v2ray.ang.service

import android.content.Context
import com.v2ray.ang.AppConfig
import com.v2ray.ang.core.CoreConfigManager
import com.v2ray.ang.core.CoreNativeManager
import com.v2ray.ang.dto.RealPingEvent
import com.v2ray.ang.enums.EConfigType
import com.v2ray.ang.extension.isComplexType
import com.v2ray.ang.extension.isNotNullEmpty
import com.v2ray.ang.handler.MmkvManager
import com.v2ray.ang.handler.SettingsManager
import com.v2ray.ang.handler.SpeedtestManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

internal object RealPingExecutionLimiter {
    private val customConfigMutex = Mutex()

    suspend fun <T> run(configType: EConfigType, block: () -> T): T {
        // Custom profiles bypass speed-test trimming and start complete Xray configs.
        // Parallel teardown can abort the native probe process, so serialize their
        // JNI measurements globally across batches.
        return if (configType == EConfigType.CUSTOM) {
            customConfigMutex.withLock { block() }
        } else {
            block()
        }
    }
}

/**
 * Worker that runs a batch of real-ping tests independently.
 * Each batch owns its own CoroutineScope/dispatcher and can be cancelled separately.
 */
class RealPingWorkerService(
    private val context: Context,
    private val guids: List<String>,
    private val onlyTcp: Boolean = false,
    private val bandwidth: Boolean = false,
    private val onEvent: (RealPingEvent) -> Unit = {}
) {
    private val job = SupervisorJob()
    // Bandwidth tests run one at a time: parallel downloads share the same uplink and skew each other.
    private val concurrency = if (bandwidth) 1 else SettingsManager.getRealPingConcurrency()
    private val dispatcher = Executors.newFixedThreadPool(if (onlyTcp) concurrency * 2 else concurrency).asCoroutineDispatcher()
    private val scope = CoroutineScope(job + dispatcher + CoroutineName("RealPingBatchWorker"))

    private val runningCount = AtomicInteger(0)
    private val stopRequested = AtomicBoolean(false)
    private val passedCount = AtomicInteger(0)
    private val bandwidthPassKbps = if (bandwidth) SettingsManager.getBandwidthPassKbps() else 0L
    private val bandwidthStopAfterPassed = if (bandwidth) SettingsManager.getBandwidthStopAfterPassed() else 0
    private val bandwidthTestBytes = if (bandwidth) SettingsManager.getBandwidthTestBytes() else 0L
    private val totalCount = AtomicInteger(0)

    fun start() {
        val jobs = guids.map { guid ->
            totalCount.incrementAndGet()
            scope.launch {
                runningCount.incrementAndGet()
                try {
                    // Stop requested (manually, or enough servers passed): skip the rest, keep results so far.
                    if (stopRequested.get()) return@launch
                    if (bandwidth) {
                        val kbps = startBandwidth(guid)
                        if (bandwidthStopAfterPassed > 0 && kbps >= bandwidthPassKbps &&
                            passedCount.incrementAndGet() >= bandwidthStopAfterPassed
                        ) {
                            stopRequested.set(true)
                        }
                        if (scope.isActive) {
                            onEvent(RealPingEvent.Result(guid, 0L, kbps))
                        }
                    } else {
                        val result = if (onlyTcp) startTcping(guid) else startRealPing(guid)
                        if (scope.isActive) {
                            onEvent(RealPingEvent.Result(guid, result))
                        }
                    }
                } catch (_: Throwable) {
                    // ignore
                } finally {
                    val count = totalCount.decrementAndGet()
                    val left = runningCount.decrementAndGet()
                    if (scope.isActive) {
                        onEvent(RealPingEvent.Progress("$left / $count"))
                    }
                }
            }
        }

        scope.launch {
            try {
                joinAll(*jobs.toTypedArray())
                if (isActive) {
                    onEvent(RealPingEvent.Finish("0"))
                }
            } catch (_: CancellationException) {
                // If cancelled, don't send finish event to avoid confusion
            } finally {
                close()
            }
        }
    }

    fun cancel() {
        job.cancel()
    }

    /**
     * Gracefully stop: the test already running finishes, the remaining servers are skipped,
     * and the batch still reports Finish so results gathered so far are kept.
     */
    fun requestStop() {
        stopRequested.set(true)
    }

    private fun close() {
        try {
            dispatcher.close()
        } catch (_: Throwable) {
            // ignore
        }
    }

    private suspend fun startRealPing(guid: String): Long {
        val retFailure = -1L

        val config = MmkvManager.decodeServerConfig(guid) ?: return retFailure
        if (!config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.startsWith("h3") != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val url = config.server.orEmpty()
            val port = config.serverPort.orEmpty().toInt()
            val tcpTime = SpeedtestManager.socketConnectTime(url, port, 1000)
            if (tcpTime <= -1L) {
                return retFailure
            }
        }

        val configResult = CoreConfigManager.getV2rayConfig4Speedtest(context, guid)
        if (!configResult.status) {
            return retFailure
        }
        return RealPingExecutionLimiter.run(config.configType) {
            CoreNativeManager.measureOutboundDelay(configResult.content, SettingsManager.getDelayTestUrl())
        }
    }

    private suspend fun startBandwidth(guid: String): Long {
        val retFailure = -1L

        val config = MmkvManager.decodeServerConfig(guid) ?: return retFailure
        // Same cheap TCP reachability pre-check as real ping, so dead servers fail fast.
        if (!config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.startsWith("h3") != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val url = config.server.orEmpty()
            val port = config.serverPort.orEmpty().toInt()
            val tcpTime = SpeedtestManager.socketConnectTime(url, port, 1000)
            if (tcpTime <= -1L) {
                return retFailure
            }
        }

        val configResult = CoreConfigManager.getV2rayConfig4Speedtest(context, guid)
        if (!configResult.status) {
            return retFailure
        }
        return RealPingExecutionLimiter.run(config.configType) {
            CoreNativeManager.measureOutboundBandwidth(
                configResult.content,
                AppConfig.BANDWIDTH_TEST_URL_BASE + bandwidthTestBytes,
                bandwidthTestBytes,
                AppConfig.BANDWIDTH_TEST_MAX_DOWNLOAD_MS
            )
        }
    }

    private fun startTcping(guid: String): Long {
        val retFailure = -1L

        val config = MmkvManager.decodeServerConfig(guid) ?: return retFailure
        if (!config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.split(',')?.all { it.trim().startsWith("h3") } != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val url = config.server.orEmpty()
            val port = config.serverPort.orEmpty().toInt()
            val tcpTime = SpeedtestManager.socketConnectTime(url, port, 1000)

            return tcpTime
        }

        return retFailure
    }
}
