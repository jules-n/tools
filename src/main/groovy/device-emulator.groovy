#!/usr/bin/env groovy
@Grapes([

        // comment it out to run/debug in IDE (to get rid of "No suitable ClassLoader found for grab" error)
        @GrabConfig(systemClassLoader = true),

        @Grab('org.codehaus.groovy:groovy-cli-picocli:3.0.8'),
        @Grab('info.picocli:picocli:4.6.1'),
        @Grab('io.vertx:vertx-core:4.1.2'),
        @Grab('io.vertx:vertx-web-client:4.1.2')
])

import groovy.cli.picocli.CliBuilder
import groovy.cli.picocli.OptionAccessor
import groovy.json.JsonOutput
import groovy.transform.ToString
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.TimeoutStream
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.eventbus.MessageProducer
import io.vertx.core.http.HttpMethod
import io.vertx.core.impl.cpu.CpuCoreSensor
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions

import javax.naming.OperationNotSupportedException
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.function.Function

import static DataPattern.RANDOM
import static DeviceType.TEMPERATURE_READ
import static EventFormat.JSON
import static TransportType.HTTP
import static io.vertx.core.http.HttpMethod.POST
import static java.lang.System.err
import static java.util.UUID.randomUUID

// cli definition
cli = new CliBuilder(name: 'device-emulator')
cli.parser.caseInsensitiveEnumValuesAllowed(true)
cli.h(longOpt: 'help', 'show usage')
cli.t(longOpt: 'type', args: 1, type: DeviceType, argName: 'type', defaultValue: 'temperature_read',
        'device type (default: temperature)')
cli.s(longOpt: 'transport-type', args: 1, type: TransportType, argName: 'type', defaultValue: 'http',
        'output transport type')
cli.f(longOpt: 'format', args: 1, type: EventFormat, argName: 'type', defaultValue: 'json',
        'event format (default: json)')
cli._(longOpt: 'url', args: 1, type: String, argName: 'url', defaultValue: 'http://localhost:8100',
        'url to send device events to')
cli.n(longOpt: 'num-devices', args: 1, type: Integer, argName: 'num', defaultValue: '1', 'number of devices to emulate')
cli.p(longOpt: 'data-pattern', args: 1, type: DataPattern, argName: 'pattern-type', defaultValue: 'random',
        'generated data pattern (default: random)')
cli._(longOpt: 'from', args: 1, argName: 'value', type: BigDecimal, defaultValue: '0',
        'lower boundary of generated data range')
cli._(longOpt: 'to', args: 1, argName: 'value', type: BigDecimal, defaultValue: '100',
        'upper boundary of generated data range')
cli.i(longOpt: 'interval', args: 1, argName: 'duration', defaultValue: '5s', 'interval between events')
cli.d(longOpt: 'duration', args: 1, argName: 'duration', 'how long to generate events')
cli.e(longOpt: 'num-events', args: 1, argName: 'num', type: Long, 'how many events each device should generate')
cli.m(longOpt: 'max-total-events', args: 1, argName: 'num', type: Long, 'maximum total events being sent out')
cli.v(longOpt: 'v', 'verbose output')
cli.vv(longOpt: 'vv', 'more verbose output')
cli.vvv(longOpt: 'vvv', 'most verbose output')


// parse and validate arguments
cliOpts = cli.parse args
if (!cliOpts) {
    return
}
if (cliOpts.h) {
    cli.usage()
    return
}

deviceType = cliOpts.t
transport = cliOpts.s
eventFormat = cliOpts.f
url = cliOpts.url
numDevices = cliOpts.n
dataPattern = cliOpts.p
verbosity = cliOpts.vvv ? [true] * 4
        : cliOpts.vv ? [true] * 3 + [false]
        : cliOpts.v ? [true] * 2 + [false] * 2
        : [false] * 4
interval = Duration.parse "PT${cliOpts.i}"
duration = cliOpts.d ? Duration.parse("PT${cliOpts.d}") : null

if (interval <= Duration.ZERO) {
    return error(IllegalArgumentException, "interval should be positive: ${interval}")
}
if (duration && duration <= Duration.ZERO) {
    return error(IllegalArgumentException, "duration should be positive: ${duration}")
}
if (deviceType !in [TEMPERATURE_READ]) {
    return error(OperationNotSupportedException, "device type not supported yet: ${deviceType}")
}
if (transport !in [HTTP]) {
    return error(OperationNotSupportedException, "output transport type not supported yet: ${transport}")
}
if (eventFormat !in [JSON]) {
    return error(OperationNotSupportedException, "event format not supported yet: ${eventFormat}")
}
if (dataPattern !in [RANDOM]) {
    return error(OperationNotSupportedException, "generated data pattern not supported yet: ${dataPattern}")
}
if (numDevices <= 0) {
    return error(IllegalArgumentException, "number of devices should be positive: ${numDevices}")
}
if (transport == HTTP) {
    try {
        new URL(url)
    } catch (anyErr) {
        return error(IllegalArgumentException, "malformed url: ${url}")
    }
}


// generate and send events
def cfg = AppConfig.buildFromCliOpts cliOpts
if (cfg.verbosity[3]) {
    println "app cfg: ${cfg}"
}
addShutdownHook {
    if (!cfg.verbosity[1]) {
        println() // new line on "Ctrl+C" forced stop
    }
}
def appFinishLatch = new CountDownLatch(1)
def availableCpus = CpuCoreSensor.availableProcessors()
def vertxOpts = new VertxOptions().tap {
    eventLoopPoolSize = availableCpus
    workerPoolSize = availableCpus > 3 ? (availableCpus / 2).round().toInteger() : availableCpus
}
if (verbosity[1]) {
    println "vertx threads config: elThreads=${vertxOpts.eventLoopPoolSize}, workerThreads=${vertxOpts.workerPoolSize}"
}
vertx = Vertx.vertx(vertxOpts)
def ebSafePublishingLocalOnlyCodec = new SafePublishingLocalOnlyCodec()
vertx.eventBus().registerCodec(ebSafePublishingLocalOnlyCodec)
def coordinator = new CoordinatorActor(cfg: cfg, doOnComplete: {
    if (cfg.verbosity[2]) {
        println "closing vertx..."
    }
    vertx.close {
        if (cfg.verbosity[2]) {
            println "vertx closed"
        }
        appFinishLatch.countDown()
    }
})
vertx.deployVerticle coordinator

appFinishLatch.await()
if (cfg.verbosity[1]) {
    println "done"
}

// ********************************************************************************************************************

@ToString(includeNames = true)
class AppConfig {
    List<Boolean> verbosity
    DataPattern dataPattern
    DeviceType deviceType
    EventFormat eventFormat
    TransportType transportType
    Duration timeToRun = null
    Duration eventInterval = Duration.ofSeconds(5)
    Long maxEvents = -1
    Long globalMaxEvents = -1
    Integer numOfGenerators = 1
    Integer numOfSenders = 1
    BigDecimal from = 0
    BigDecimal to = 100
    String url
    Duration progressBarRefreshInterval = Duration.ofMillis(250)
    Integer defaultTerminalWidth = 80

    static AppConfig buildFromCliOpts(OptionAccessor cliOpts) {
        new AppConfig().tap {
            deviceType = cliOpts.t
            transportType = cliOpts.s
            eventFormat = cliOpts.f
            url = cliOpts.url
            numOfGenerators = cliOpts.n
            dataPattern = cliOpts.p
            from = cliOpts.from
            to = cliOpts.to
            verbosity = cliOpts.vvv ? [true] * 4
                    : cliOpts.vv ? [true] * 3 + [false]
                    : cliOpts.v ? [true] * 2 + [false] * 2
                    : [false] * 4
            eventInterval = Duration.parse "PT${cliOpts.i}"
            timeToRun = cliOpts.d ? Duration.parse("PT${cliOpts.d}") : null
            maxEvents = cliOpts.e
            globalMaxEvents = cliOpts.m
            numOfSenders = Runtime.runtime.availableProcessors() + 1
        }
    }
}

interface Generator {
    double generate()
}

interface EventBuilder {
    def buildEvent(double value)
}

interface EventFormatter {
    byte[] formatEvent(def event)
}

interface EventSender {
    Future<Object> sendEvent(byte[] formattedEvent)
}

class GeneratorProvider {
    Generator create(AppConfig cfg) {
        def generator
        switch (cfg.dataPattern) {
            case RANDOM:
                generator = new RandomGenerator()
                break
            default:
                throw new UnsupportedOperationException("unsupported data generation pattern: ${cfg.dataPattern}")
        }
        generator
    }
}

class EventBuilderProvider {
    EventBuilder create(AppConfig cfg) {
        def eventBuilder
        switch (cfg.deviceType) {
            case TEMPERATURE_READ:
                String vendor = "ss-devices"
                eventBuilder = new TemperatureEventBuilder(deviceId: randomUUID().toString(), range: cfg.from..cfg.to)
                break
            default:
                throw new UnsupportedOperationException("unsupported device type: ${cfg.deviceType}")
        }
        eventBuilder
    }
}

class EventFormatterProvider {
    @Lazy
    static JsonEventFormatter JSON_EVENT_FORMATTER

    EventFormatter create(AppConfig cfg) {
        def formatter
        switch (cfg.eventFormat) {
            case JSON:
                formatter = JSON_EVENT_FORMATTER
                break
            default:
                throw new UnsupportedOperationException("unsupported event format: ${cfg.eventFormat}")
        }
        formatter
    }
}

class EventSenderProvider {
    @Lazy
    WebClient webClient = {
        def vertx = Vertx.currentContext().owner()
        WebClient.create vertx, new WebClientOptions().tap {
            keepAlive = false
            connectTimeout = 5_000
            sendBufferSize = -1
        }
    }()

    EventSender create(AppConfig cfg) {
        def sender
        switch (cfg.transportType) {
            case HTTP:
                sender = new HttpEventSender(cfg: cfg, httpMethod: POST, webClient: webClient)
                break
            default:
                throw new UnsupportedOperationException("unsupported transport: ${cfg.transportType}")
        }
        sender
    }
}

class RandomGenerator implements Generator {
    private Random rnd = new Random()

    @Override
    double generate() {
        rnd.nextDouble()
    }
}

class TemperatureEventBuilder implements EventBuilder {
    String deviceId
    NumberRange range

    @Override
    def buildEvent(double value) {
        def rangeSize = range.to - range.from
        def sizedValue = rangeSize * value
        def temperature = range.from + sizedValue
        new TemperatureReadEvent(deviceId: deviceId, temperature: temperature)
    }
}

class JsonEventFormatter implements EventFormatter {
    @Override
    byte[] formatEvent(Object event) {
        JsonOutput.toJson(event).getBytes(StandardCharsets.UTF_8)
    }
}

class HttpEventSender implements EventSender {
    AppConfig cfg
    HttpMethod httpMethod
    WebClient webClient

    private String host
    private String uriPath
    private int port

    @Override
    Future<Object> sendEvent(byte[] formattedEvent) {
        webClient.request(httpMethod, port, host, uriPath)
                .sendBuffer(Buffer.buffer(formattedEvent))
                .map({ it.bodyAsString() } as Function)
    }

    void setCfg(AppConfig cfg) {
        def url = new URL(cfg.url)
        host = url.host
        uriPath = url.path
        port = url.port != -1 ? url.port : 80
        this.@cfg = cfg
    }
}

class CoordinatorActor extends AbstractVerticle {
    static final String ADDR_INC_NUM_GENERATED_EVENTS = 'coordinator.inc-num-generated-events'
    static final String ADDR_GENERATOR_FINISHED = 'coordinator.generator-finished'
    static final String ADDR_INC_NUM_SENT_EVENTS = 'coordinator.inc-num-sent-events'

    AppConfig cfg
    String incNumGeneratedEventsAddr = ADDR_INC_NUM_GENERATED_EVENTS
    String generatorFinishedAddr = ADDR_GENERATOR_FINISHED
    String incNumSentEventsAddr = ADDR_INC_NUM_SENT_EVENTS
    GeneratorProvider generatorProvider = new GeneratorProvider()
    EventBuilderProvider eventBuilderProvider = new EventBuilderProvider()
    EventFormatterProvider eventFormatterProvider = new EventFormatterProvider()
    EventSenderProvider eventSenderProvider = new EventSenderProvider()
    Handler<AsyncResult<Void>> doOnComplete = {} as Handler

    private EventBus eb
    private MessageConsumer<Void> incNumGeneratedEventsConsumer
    private MessageConsumer<Void> generatorFinishedConsumer
    private MessageConsumer<Void> incNumSentEventsConsumer
    private Set<String> generatorsDeployments = []
    private Set<String> sendersDeployments = []
    private long numGeneratedEvents = 0
    private long numSentEvents = 0
    private long numActiveGenerators = 0
    private boolean isDisposed = false
    private long globalMaxEvents
    private Long progressBarTimerId
    private boolean isAnsiTerminal = true
    private Instant startedAt

    @Override
    void start(Promise verticleStarted) {
        eb = vertx.eventBus()
        registerIncNumGeneratedEventsConsumer()
        registerGeneratorFinishedConsumer()
        registerIncNumSentEventsConsumer()
        if (!cfg.verbosity[1]) {
            startProgressBar()
        }
        deploySenders()
                .compose { deployGenerators() }
                .onComplete {
                    startedAt = Instant.now()
                    if (cfg.verbosity[2]) {
                        println "coordinator started: generatorsDeployed=${generatorsDeployments.size()}, " +
                                "sendersDeployed=${sendersDeployments.size()}"
                    }
                    verticleStarted.handle it
                }
    }

    @Override
    void stop(Promise verticleStopped) {
        dispose().onComplete(verticleStopped)
    }

    private Future<Void> deploySenders() {
        def senderActors = (0..<cfg.numOfSenders).collect {
            new EventSenderActor(cfg: cfg, eventSender: eventSenderProvider.create(cfg))
        }
        def sendersDeploymentsFutures = senderActors.collect {
            vertx.deployVerticle(it).map({
                sendersDeployments << it
            } as Function)
        }
        CompositeFuture.all(sendersDeploymentsFutures).map(null)
    }

    private Future<Void> deployGenerators() {
        def rnd = new Random()
        def generatorActors = (0..<cfg.numOfGenerators).collect {
            new EventGeneratorActor(cfg: cfg,
                    generator: generatorProvider.create(cfg),
                    eventBuilder: eventBuilderProvider.create(cfg),
                    eventFormatter: eventFormatterProvider.create(cfg),
                    startDelay: Duration.ofMillis(rnd.nextInt(cfg.eventInterval.toMillis() as int) + 1)
            )
        }
        def generatorsDeploymentsFutures = generatorActors.collect {
            vertx.deployVerticle(it).map({
                generatorsDeployments << it
                numActiveGenerators++
            } as Function)
        }
        CompositeFuture.all(generatorsDeploymentsFutures).map(null)
    }

    private void registerIncNumGeneratedEventsConsumer() {
        globalMaxEvents = cfg.globalMaxEvents != null ? cfg.globalMaxEvents : -1
        incNumGeneratedEventsConsumer = eb.consumer(incNumGeneratedEventsAddr) {
            numGeneratedEvents++
            def globalMaxEventsReached = (globalMaxEvents != -1)
                    ? globalMaxEvents < numGeneratedEvents
                    : false
            it.reply globalMaxEventsReached
        }
    }

    private void registerGeneratorFinishedConsumer() {
        generatorFinishedConsumer = eb.consumer(generatorFinishedAddr) {
            if (cfg.verbosity[2]) {
                println "generator finished: depId=${it.body()}"
            }
            numActiveGenerators--
            checkIfAllFinished()
        }
    }

    private void registerIncNumSentEventsConsumer() {
        incNumSentEventsConsumer = eb.consumer(incNumSentEventsAddr) {
            numSentEvents++
            checkIfAllFinished()
        }
    }

    private void checkIfAllFinished() {
        def noActiveGenerators = !numActiveGenerators
        def hasGlobalMaxEvents = globalMaxEvents != -1
        def hasPerDeviceMaxEvents = cfg.maxEvents != null && cfg.maxEvents != -1
        def globalMaxEventsReached = hasGlobalMaxEvents && numSentEvents >= globalMaxEvents
        def perDeviceMaxEventsReached = hasPerDeviceMaxEvents && numSentEvents >= cfg.maxEvents * cfg.numOfGenerators
        def noUnsentMessages = hasGlobalMaxEvents || hasPerDeviceMaxEvents
                ? globalMaxEventsReached || perDeviceMaxEventsReached
                : numSentEvents == numGeneratedEvents
        def allFinished = noActiveGenerators && noUnsentMessages
        if (numSentEvents > numGeneratedEvents) {
            err.println "BUG: numSentEvents counter can not be greater than numGeneratedEvents: " +
                    "numSentEvents=${numSentEvents}, numGeneratedEvents=${numGeneratedEvents}"
        }
        if (cfg.verbosity[3]) {
            println "check if all finished: allFinished=${allFinished}, " +
                    "numActiveGenerators=${numActiveGenerators}, numSentEvents=${numSentEvents}, " +
                    "numGeneratedEvents=${numGeneratedEvents}"
        }
        if (allFinished) {
            if (cfg.verbosity[1]) {
                println "sending events finished, disposing all"
            }
            dispose()
        }
    }

    private Future<Void> dispose() {
        if (isDisposed) {
            if (cfg.verbosity[3]) {
                println "duplicated dispose call, ignoring"
            }
            return Future.succeededFuture()
        }
        def incNumGeneratedEventsUnreg = incNumGeneratedEventsConsumer.unregister().onComplete {
            if (cfg.verbosity[3]) {
                println "eb consumer unregistered - incNumGeneratedEvents"
            }
        }
        def generatorFinishedUnreg = generatorFinishedConsumer.unregister().onComplete {
            if (cfg.verbosity[3]) {
                println "eb consumer unregistered - generatorFinished"
            }
        }
        def incNumSentEventsUnreg = incNumSentEventsConsumer.unregister().onComplete {
            if (cfg.verbosity[3]) {
                println "eb consumer unregistered - incNumSentEvents"
            }
        }
        def generatorsUndeploy = generatorsDeployments.collect { genDepId ->
            vertx.undeploy(genDepId).onComplete {
                if (cfg.verbosity[3]) {
                    println "generator undeployed: success=${it.succeeded()}, depId=${genDepId}"
                }
            }
        }
        def sendersUndeploy = sendersDeployments.collect { senderDepId ->
            vertx.undeploy(senderDepId).onComplete {
                if (cfg.verbosity[3]) {
                    println "sender undeployed: success=${it.succeeded()}, depId=${senderDepId}"
                }
            }
        }
        def disposeAllResources = [incNumGeneratedEventsUnreg, generatorFinishedUnreg, incNumSentEventsUnreg] +
                generatorsUndeploy + sendersUndeploy
        isDisposed = true
        CompositeFuture.all(disposeAllResources)
                .onComplete {
                    if (!cfg.verbosity[1]) {
                        stopProgressBar()
                    }
                    if (cfg.verbosity[1]) {
                        println "all disposed, ready to close vertx"
                    }
                }
                .<Void> map(null) // ignore completion status report
                .onComplete(doOnComplete)
    }

    private void startProgressBar() {
        def hasMaxGlobalEventsSet = globalMaxEvents != -1
        def hasMaxEventsPerDeviceSet = cfg.maxEvents != -1 && cfg.maxEvents != null
        def hasTimeLimit = cfg.timeToRun as Boolean
        def maxTimeToRun = hasTimeLimit ? cfg.timeToRun + cfg.eventInterval : null
        def isInfinite = !hasMaxGlobalEventsSet && !hasMaxEventsPerDeviceSet && !hasTimeLimit
        progressBarTimerId = vertx.setPeriodic(cfg.progressBarRefreshInterval.toMillis()) {
            if (isInfinite) {
                print "\rgenerated=${numGeneratedEvents} sent=${numSentEvents}"
            } else {
                getTerminalWidth().onComplete {
                    def termWidth = it.result()

                    def (pcntGlobalEvents, pcntPerDeviceEvents, pcntTime) = [0.0] * 3
                    def (globalEventsStr, perDeviceEventsStr, timeStr) = [''] * 3
                    if (hasMaxGlobalEventsSet) {
                        pcntGlobalEvents = (globalMaxEvents != 0) ? (numSentEvents / globalMaxEvents) * 100 : 100
                        globalEventsStr = " maxTotal=${globalMaxEvents}"
                    }
                    if (hasMaxEventsPerDeviceSet) {
                        def maxPerAllDevices = cfg.numOfGenerators * cfg.maxEvents
                        pcntPerDeviceEvents = (numSentEvents / maxPerAllDevices) * 100
                        perDeviceEventsStr = " maxPerDevice=${maxPerAllDevices}" +
                                "(${cfg.maxEvents}x${cfg.numOfGenerators})"
                    }
                    if (hasTimeLimit) {
                        def now = Instant.now()
                        def timePassed = Duration.between startedAt, now
                        pcntTime = (timePassed.toMillis() / maxTimeToRun.toMillis()) * 100
                        if (pcntTime > 100) {
                            pcntTime = 100
                        }
                        timeStr = " time=${timePassed.toSeconds()}/${maxTimeToRun.toSeconds()}"
                    }
                    def pcntProgress = [pcntGlobalEvents, pcntPerDeviceEvents, pcntTime].max()
                    def dataMsgPart = " ${pcntProgress.trunc(2)}% gen=${numGeneratedEvents} sent=${numSentEvents}" +
                            timeStr + perDeviceEventsStr + globalEventsStr
                    def progressBarMsgPart = ''
                    def progressBarSize = termWidth - 1 - dataMsgPart.size()
                    if (progressBarSize >= 5) {
                        def numTicks = progressBarSize - 2
                        def numCompletedTicks = (numTicks * (pcntProgress / 100)).round().toInteger()
                        def numIncompleteTicks = numTicks - numCompletedTicks
                        progressBarMsgPart = "[${'#' * numCompletedTicks}${'-' * numIncompleteTicks}]"
                    }

                    def progressMsg = progressBarMsgPart + dataMsgPart
                    def formattedProgressMsg = "\r${progressMsg.padRight(termWidth - 1)}".toString()
                    print formattedProgressMsg
                }
            }
        }
    }

    private void stopProgressBar() {
        if (progressBarTimerId) {
            vertx.cancelTimer(progressBarTimerId)
        }
    }

    private Future<Integer> getTerminalWidth() {
        if (!isAnsiTerminal) {
            Future.succeededFuture cfg.defaultTerminalWidth
        } else {
            vertx.executeBlocking { result ->
                def proc = 'tput cols 2> /dev/tty'.execute()
                proc.waitFor()
                def resStr = proc.text.trim()
                if (resStr.integer) {
                    result.complete resStr.toInteger()
                } else {
                    isAnsiTerminal = false
                    result.complete cfg.defaultTerminalWidth
                }
            }
        }
    }
}

class EventGeneratorActor extends AbstractVerticle {
    AppConfig cfg
    Generator generator
    EventBuilder eventBuilder
    EventFormatter eventFormatter
    Duration startDelay = Duration.ZERO
    String sendEventAddr = EventSenderActor.ADDR_SEND_EVENT
    String generatorFinishedAddr = CoordinatorActor.ADDR_GENERATOR_FINISHED
    String incNumGeneratedEventsAddr = CoordinatorActor.ADDR_INC_NUM_GENERATED_EVENTS

    private Instant shouldStopAt = null
    private long numEventsLeft = -1
    private Long initialTimerId
    private TimeoutStream ticker
    private boolean globalMaxEventsReached = false
    private boolean isRunning = false
    private MessageProducer eventSender
    private EventBus eb

    @Override
    void start() {
        if (cfg.maxEvents != null && cfg.maxEvents != -1) {
            numEventsLeft = cfg.maxEvents
        }
        eb = vertx.eventBus()
        initialTimerId = vertx.setTimer(startDelay.toMillis()) {
            initialTimerId = null
            if (!isRunning) {
                return
            }
            eventSender = eb.sender sendEventAddr, new DeliveryOptions(codecName: SafePublishingLocalOnlyCodec.NAME)
            ticker = vertx.periodicStream(cfg.eventInterval.toMillis()).handler {
                generate()
            }
            // ticker always starts with delay, so do first cycle manually right away after initial delay
            generate()
        }
        if (cfg.timeToRun) {
            shouldStopAt = Instant.now() + cfg.timeToRun
            if (cfg.verbosity[3]) {
                println "generator will stop: shouldStopAt=${shouldStopAt}, now=${Instant.now()}"
            }
        }
        isRunning = true
        if (cfg.verbosity[3]) {
            println "generator deployed: depId=${deploymentID()}"
        }
    }

    @Override
    void stop(Promise verticleStopped) {
        stopGenerating().onComplete(verticleStopped)
    }

    private void generate() {
        if (!shouldContinueGenerating()) {
            stopGenerating()
            return
        }
        var eventData = generateNewEventObject()
        if (cfg.verbosity[2]) {
            println "event data generated: generatorDepId=${deploymentID()}"
        }
        eb.request(incNumGeneratedEventsAddr, null) {
            if (it.failed()) {
                err.println "ERROR: failed to increment num of generated events: ${it.cause()}"
                return
            }
            globalMaxEventsReached = it.result().body()
            if (globalMaxEventsReached && cfg.verbosity[2]) {
                println "global max events reached, generator will stop: depId=${deploymentID()}"
            }
            if (!shouldContinueGenerating()) {
                stopGenerating()
                return
            }
            eventSender.write(eventData) { eventSendRes ->
                if (eventSendRes.failed()) {
                    err.println "ERROR: failed to pass generated event to sender: ${eventSendRes.cause()}"
                }
                if (numEventsLeft != -1) {
                    numEventsLeft--
                }
            }
        }
    }

    private Future stopGenerating() {
        def stopGeneratingFutures = []
        if (initialTimerId) {
            vertx.cancelTimer initialTimerId
            initialTimerId = null
        }
        if (ticker) {
            ticker.cancel()
            ticker = null
        }
        if (eventSender) {
            stopGeneratingFutures << eventSender.close()
            eventSender = null
        }
        isRunning = false
        (stopGeneratingFutures ? CompositeFuture.all(stopGeneratingFutures) : Future.<Void> succeededFuture())
                .onComplete { notifyGeneratorFinished() }
    }

    private def notifyGeneratorFinished() {
        eb.send generatorFinishedAddr, deploymentID(), new DeliveryOptions(codecName: SafePublishingLocalOnlyCodec.NAME)
    }

    private def shouldContinueGenerating() {
        def hasTimedOut = shouldStopAt ? Instant.now() > shouldStopAt : false
        def maxNumEventsReached = numEventsLeft != -1 ? numEventsLeft <= 0 : false
        isRunning && !hasTimedOut && !maxNumEventsReached && !globalMaxEventsReached
    }

    private def generateNewEventObject() {
        def value = generator.generate()
        def event = eventBuilder.buildEvent value
        eventFormatter.formatEvent event
    }
}

class EventSenderActor extends AbstractVerticle {
    static final String ADDR_SEND_EVENT = 'event-sender.send-event'

    AppConfig cfg
    EventSender eventSender
    String eventSenderAddr = ADDR_SEND_EVENT
    String incNumSentEventsAddr = CoordinatorActor.ADDR_INC_NUM_SENT_EVENTS

    private EventBus eb
    private MessageConsumer eventSenderConsumer
    private boolean shouldStop = false
    private long inFlightEvents = 0
    private Promise<Void> stoppedSendingEvents = Promise.promise()

    @Override
    void start() {
        eb = vertx.eventBus()
        eventSenderConsumer = eb.<byte[]> consumer(eventSenderAddr) {
            eventSender.sendEvent(it.body())
                    .tap { inFlightEvents++ }
                    .onComplete {
                        inFlightEvents--
                        notifyEventSent()
                        if (it.failed() && cfg.verbosity[1]) {
                            err.println "ERROR: failed to send event: err=${it.cause()}"
                            if (cfg.verbosity[2]) {
                                it.cause().printStackTrace err
                            }
                        }
                        checkIfFinished()
                    }
        }
        if (cfg.verbosity[3]) {
            println "sender deployed: depId=${deploymentID()}"
        }
    }

    @Override
    void stop(Promise verticleStopped) {
        def eventSenderUnreg = eventSenderConsumer.unregister()
        shouldStop = true
        checkIfFinished()
        CompositeFuture.all(eventSenderUnreg, stoppedSendingEvents.future())
                .onComplete verticleStopped
    }

    private void checkIfFinished() {
        if (shouldStop && !inFlightEvents) {
            stoppedSendingEvents.complete()
        }
    }

    private void notifyEventSent() {
        eb.send incNumSentEventsAddr, null, new DeliveryOptions(codecName: SafePublishingLocalOnlyCodec.NAME)
    }
}

class TemperatureReadEvent {
    String deviceId
    String type = 'temperature-read'
    BigDecimal temperature
}

enum DataPattern {
    RANDOM
}

enum DeviceType {
    TEMPERATURE_READ
}

enum EventFormat {
    JSON
}

enum TransportType {
    HTTP,
    MQTT
}

class SafePublishingLocalOnlyCodec implements MessageCodec<Object, Object> {
    public static final String NAME = SafePublishingLocalOnlyCodec.class.simpleName

    private volatile Object dataRef

    @Override
    void encodeToWire(Buffer buffer, Object o) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object decodeFromWire(int pos, Buffer buffer) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object transform(Object data) {
        dataRef = data
    }

    @Override
    String name() {
        NAME
    }

    @Override
    byte systemCodecID() {
        -1
    }
}

def error(Class<? extends Throwable> type, CharSequence msg) {
    if (verbosity[1]) {
        throw type.newInstance(new Object[] {msg.toString()})
    } else {
        err.println "ERROR: ${msg}"
        return 1
    }
}
