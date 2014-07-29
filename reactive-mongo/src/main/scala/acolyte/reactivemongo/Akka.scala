package acolyte.reactivemongo

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }

import com.typesafe.config.Config
import akka.actor.{ ActorRef, ActorSystem ⇒ AkkaSystem, Props }

import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers }
import reactivemongo.core.actors.{
  Close,
  CheckedWriteRequestExpectingResponse,
  RequestMakerExpectingResponse
}
import reactivemongo.core.protocol.{
  Query,
  MessageHeader,
  Request,
  RequestMaker,
  RequestOp,
  Reply,
  Response,
  ResponseInfo
}
import reactivemongo.bson.BSONDocument
import reactivemongo.bson.buffer.{
  ArrayReadableBuffer,
  ReadableBuffer
}
import reactivemongo.core.netty.{ BufferSequence, ChannelBufferWritableBuffer }

/** Akka companion for Acolyte mongo system. */
private[reactivemongo] object Akka {
  /**
   * Creates an Acolyte actor system for ReactiveMongo use.
   *
   * {{{
   * import acolyte.reactivemongo.MongoSystem
   * import akka.actor.ActorSystem
   *
   * val mongo: ActorSystem = Akka.actorSystem()
   * }}}
   *
   * @param name Actor system name (default: "ReactiveMongoAcolyte")
   */
  def actorSystem(name: String = "ReactiveMongoAcolyte"): AkkaSystem = new ActorSystem(AkkaSystem(name), new ActorRefFactory() {
    def before(system: AkkaSystem, then: ActorRef): ActorRef = {
      system actorOf Props(classOf[Actor], then)
    }
  })
}

final class Actor(then: ActorRef) extends akka.actor.Actor {
  @annotation.tailrec
  def go(chan: ChannelBuffer, body: Array[Byte] = Array(), enc: String = "UTF-8"): Array[Byte] = {
    val len = chan.readableBytes()

    if (len == 0) body
    else {
      val buff = new Array[Byte](len)
      chan.readBytes(buff)

      go(chan, body ++ buff, enc)
    }
  }

  def receive = {
    case msg @ CheckedWriteRequestExpectingResponse(req) ⇒
      println(s"wreq = $req")
      then forward msg

    case msg @ RequestMakerExpectingResponse(RequestMaker(op @ Query(_ /*flags*/ , coln, off, len), doc, _ /*pref*/ , _ /*channelId*/ )) ⇒
      val exp = new ExpectingResponse(msg)
      /*
      val readable = ArrayReadableBuffer(go(doc.merged))
      val bson = DefaultBufferHandler.readDocument(readable)

      println(s"""expecting = Query($coln, $off, $len), [${bson.map(_.elements)}]""")
       */

      val resp = mkResponse(op, List(BSONDocument("toto" -> "tata")))
      println(s"""resp = ${BSONDocument("toto" -> "tata").elements.toList}""")

      import scala.concurrent.ExecutionContext.Implicits.global
      import scala.util.Failure
      exp.future map { r ⇒
        println(s"r = ${Response.parse(r).toList}")
        val data = ArrayReadableBuffer(r.documents.array)
        println(s"data = ${data.size}");
        println(s"=> ${BSONDocument.read(data).elements.toList}")
        r
      }

      //exp.promise.success(resp)
    then forward msg

    case close @ Close ⇒ /* Do nothing */ then forward close
    case msg ⇒
      println(s"message = $msg")
      then forward msg
  }

  private def mkResponse(op: RequestOp, docs: TraversableOnce[BSONDocument]): Response = {
    val (len, seq) = docs.foldLeft(0 -> Seq[ChannelBuffer]()) { (st, d) ⇒
      val (l, s) = st

      val o = new ChannelBufferWritableBuffer()
      BSONDocument.write(d, o)

      (l + o.index) -> (s :+ o.buffer)
    }
    val bufseq = seq.headOption.fold(BufferSequence.empty)(
      BufferSequence(_, seq.tail: _*))

    val outbuf = bufseq.merged

    println(s"len = $len")

    val header = MessageHeader(
      messageLength = len,
      requestID = System.identityHashCode(docs),
      responseTo = System.identityHashCode(outbuf),
      opCode = op.code)

    val resp = Response(header, Reply(outbuf), outbuf, ResponseInfo(1))

    println(s"!!!! => ${Response.parse(resp)}")
    resp
  }
}
