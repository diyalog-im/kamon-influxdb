package kamon.influxdb

import java.time.Instant

import com.typesafe.config.Config
import kamon.influxdb.InfluxDBReporter.Settings
import kamon.metric.{MetricDistribution, MetricValue, PeriodSnapshot}
import kamon.util.EnvironmentTagBuilder
import kamon.{Kamon, MetricReporter}
import okhttp3.{Authenticator, Credentials, MediaType, OkHttpClient, Request, RequestBody, Response, Route}
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Try

class InfluxDBReporter(config: Config = Kamon.config()) extends MetricReporter {
  private val logger = LoggerFactory.getLogger(classOf[InfluxDBReporter])
  private var settings = InfluxDBReporter.readSettings(config)
  private val client : Either[OkHttpClient, KafkaProducer[String,String]] = buildClient(settings)

  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {

    Try {
      client match {
        case Right(kafkaClient) =>

            val data = new ProducerRecord[String, String](settings.kafkaSettings.get.topic, settings.kafkaSettings.get.key, createData(snapshot))
            kafkaClient.send(data)

        case Left(httpClient) =>
          val request = new Request.Builder()
            .url(settings.url)
            .post(translateToLineProtocol(snapshot))
            .build()

            val response = httpClient.newCall(request).execute()
            if(response.isSuccessful())
              logger.trace("Successfully sent metrics to InfluxDB")
            else {
              logger.error("Metrics POST to InfluxDB failed with status code [{}], response body: {}",
                response.code(),
                response.body().string())
            }

            response.close()
        }
    }.failed.map {
      error => logger.error("Failed to POST metrics to InfluxDB", error)
    }

  }


  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def reconfigure(config: Config): Unit = {
    settings = InfluxDBReporter.readSettings(config)
  }

  private def translateToLineProtocol(periodSnapshot: PeriodSnapshot): RequestBody = {
    RequestBody.create(MediaType.parse("text/plain"), createData(periodSnapshot))
  }

  private def getTimeInNanoSecond(timestamp : Instant) : Long  = timestamp.getEpochSecond * 1000000000L + timestamp.getNano

  private def createData(periodSnapshot : PeriodSnapshot) : String = {
    import periodSnapshot.metrics._
    val builder = StringBuilder.newBuilder

    counters.foreach(c => writeMetricValue(builder, c, "count", getTimeInNanoSecond(periodSnapshot.to)))
    gauges.foreach(g => writeMetricValue(builder, g, "value", getTimeInNanoSecond(periodSnapshot.to)))
    histograms.foreach(h => writeMetricDistribution(builder, h, settings.percentiles, getTimeInNanoSecond(periodSnapshot.to)))
    rangeSamplers.foreach(rs => writeMetricDistribution(builder, rs, settings.percentiles, getTimeInNanoSecond(periodSnapshot.to)))

    builder.result()
  }

  private def writeMetricValue(builder: StringBuilder, metric: MetricValue, fieldName: String, timestamp: Long): Unit = {
    writeNameAndTags(builder, metric.name, metric.tags)
    writeIntField(builder, fieldName, metric.value, appendSeparator = false)
    writeTimestamp(builder, timestamp)
  }

  private def writeMetricDistribution(builder: StringBuilder, metric: MetricDistribution, percentiles: Seq[Double], timestamp: Long): Unit = {
    writeNameAndTags(builder, metric.name, metric.tags)
    writeIntField(builder, "count", metric.distribution.count)
    writeIntField(builder, "sum", metric.distribution.sum)
    writeIntField(builder, "min", metric.distribution.min)

    percentiles.foreach(p => {
      writeDoubleField(builder, "p" + String.valueOf(p), metric.distribution.percentile(p).value)
    })

    writeIntField(builder, "max", metric.distribution.max, appendSeparator = false)
    writeTimestamp(builder, timestamp)
  }

  private def writeNameAndTags(builder: StringBuilder, name: String, metricTags: Map[String, String]): Unit = {
    val convertedName =
      if (settings.prefix != "")
        s"${settings.prefix}_" + name.replace('.','_')
      else
        builder.append(name.replace('.','_'))

    builder.append(convertedName)

    val tags = if(settings.additionalTags.nonEmpty) metricTags ++ settings.additionalTags else metricTags

    if(tags.nonEmpty) {
      tags.foreach {
        case (key, value) =>
          builder
            .append(',')
            .append(escapeString(key))
            .append("=")
            .append(escapeString(value))
      }
    }

    builder.append(' ')
  }

  private def escapeString(in: String): String =
    in.replace(" ", "\\ ")
      .replace("=", "\\=")
      .replace(",", "\\,")

  def writeDoubleField(builder: StringBuilder, fieldName: String, value: Double, appendSeparator: Boolean = true): Unit = {
    builder
      .append(fieldName)
      .append('=')
      .append(String.valueOf(value))

    if(appendSeparator)
      builder.append(',')
  }

  def writeIntField(builder: StringBuilder, fieldName: String, value: Long, appendSeparator: Boolean = true): Unit = {
    builder
      .append(fieldName)
      .append('=')
      .append(String.valueOf(value))
      .append('i')

    if(appendSeparator)
      builder.append(',')
  }

  def writeTimestamp(builder: StringBuilder, timestamp: Long): Unit = {
    builder
      .append(' ')
      .append(timestamp)
      .append("\n")
  }

  protected def buildClient(settings: Settings): Either[OkHttpClient, KafkaProducer[String, String]] = {

    if (settings.kafkaEnabled){
      import java.util.Properties

      val props = new Properties()
      props.put("bootstrap.servers", settings.kafkaSettings.get.broker)
      props.put("client.id", "KamonInfluxDbKafkaPlugin")
      props.put("reconnect.backoff.ms", Int.box(10000))
      props.put("retry.backoff.ms", Int.box(10000))
      props.put("max.request.size", Int.box(settings.kafkaSettings.get.maxReqSize))
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      Right(new KafkaProducer[String, String](props))
    }
    else{
      val basicBuilder = new OkHttpClient.Builder()
      val authenticator = settings.credentials.map(credentials => new Authenticator() {
        def authenticate(route: Route, response: Response): Request = {
          response.request().newBuilder().header("Authorization", credentials).build()
        }
      })
      Left(authenticator.foldLeft(basicBuilder){ case (builder, auth) => builder.authenticator(auth)}.build())
    }

  }
}

object InfluxDBReporter {

  case class KafkaSettings(
    broker : String,
    topic : String,
    key : String,
    maxReqSize : Int
  )

  case class Settings(
    url: String,
    percentiles: Seq[Double],
    credentials: Option[String],
    additionalTags: Map[String, String],
    kafkaEnabled : Boolean,
    kafkaSettings : Option[KafkaSettings],
    prefix : String = ""
  )

  def readSettings(config: Config): Settings = {
    import scala.collection.JavaConverters._
    val root = config.getConfig("kamon.influxdb")
    val host = root.getString("hostname")
    val authConfig = Try(root.getConfig("authentication")).toOption
    val credentials = authConfig.map(conf => Credentials.basic(conf.getString("user"), conf.getString("password")))
    val port = root.getInt("port")
    val database = root.getString("database")
    val protocol = root.getString("protocol").toLowerCase
    val url = s"$protocol://$host:$port/write?precision=s&db=$database"

    val additionalTags = EnvironmentTagBuilder.create(root.getConfig("additional-tags"))

    val prefix : String = root.getString("prefix")

    val kafkaEnabled = root.getBoolean("kafka_enabled")
    val kafka = Try(root.getConfig("kafka"))
    val kafkaSettings = for {
      broker ← Try(kafka.get.getString("broker"))
      topic ← Try(kafka.get.getString("topic"))
      key ← Try(kafka.get.getString("key"))
      maxReqSize <- Try(kafka.get.getInt("max-request-size"))
    } yield KafkaSettings(broker,topic,key,maxReqSize)

    Settings(
      url,
      root.getDoubleList("percentiles").asScala.map(_.toDouble),
      credentials,
      additionalTags,
      kafkaEnabled,
      kafkaSettings.toOption,
      prefix
    )
  }
}
