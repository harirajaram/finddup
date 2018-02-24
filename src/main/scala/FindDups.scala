import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, Channel}
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.Future

import akka.actor._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer



object DirectorScanner {
  def props(): Props = Props(new DirectorScanner())

}

object ScanAndCalculateSize {

  case class Recurse(val directory: File)

  case object Scan

  case class CreateChannel(channel: AsynchronousFileChannel, file: File)

  case class CalculateSize(file: File)

  case object Finish

  case object SizeCalculationDone

  def props(manager: ActorRef, dir: File, sizeMap: TrieMap[String, Long]): Props = Props(new ScanAndCalculateSize(manager, dir, sizeMap))


}

object CheckSum {

  def props(manager: ActorRef, file: File, map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]): Props = Props(new CheckSum
  (manager, file, map))

  case class ReadFileChannel(channnel: AsynchronousFileChannel, position: Long, capacity: Long)

  case class Calculate(buffer: ByteBuffer, future: Future[Integer], channnel: Channel, position: Long)


}

object FirstRound {

  def props(sizeMap: TrieMap[String, Long], map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]): Props = Props(new FirstRound(sizeMap, map))

  case object ReadTail


}

object SecoundRound {

  def props(map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]): Props = Props(new SecoundRound(map))

  case object ReadHead

}

object FinalRound {

  def props(map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]): Props = Props(new FinalRound(map))

  case object ReadRemainingContentsOfTheFile


}


import CheckSum._
import ScanAndCalculateSize._
import FirstRound._
import SecoundRound._
import FinalRound._


// main app

object FindDups extends App {

  // get the base directory to can
  val baseDirectoryToRecurse = new File(args(0))

  //prelim checks
  if (baseDirectoryToRecurse.exists) {
    if (baseDirectoryToRecurse.isDirectory) {
      val system = ActorSystem("FindDups")
      //call the base Actor to recurse
      val baseActor = system.actorOf(Props(classOf[DirectorScanner]))
      baseActor ! Recurse(baseDirectoryToRecurse)
    } else {
      println(s"$baseDirectoryToRecurse is not even a directory")
    }
  } else {
    println(s"$baseDirectoryToRecurse doesn't exist")
  }

}

//  directory scanner
class DirectorScanner() extends Actor {

  // these are the basemaps. triemap is eqivalent to concurrenthashmap in java

  // this will be used for checkSums
  val map = new TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]
  //this will be used for size maps
  val sizeMap = new TrieMap[String, Long]

  // sub actor which will be used for futher recursion
  // for every child directory we have the subActor

  var subActors = new ArrayBuffer[ActorRef]


  def receive: Receive = {

    case Recurse(dir) =>
      // for every recurse directory create the actor
      val subActor = context.actorOf(Props(classOf[ScanAndCalculateSize], self, dir, sizeMap))
      subActors += subActor
      // send the message
      subActor ! Scan

    case SizeCalculationDone =>

      sender ! PoisonPill
      subActors -= sender

      if (subActors.isEmpty) {
        // self explantory as I'm grouping by size
        val y = sizeMap.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
        // if there is only one row.. no use as the size are not same
        y.keys.map(row => {
          if (row.size == 1) {
            // so remobe it
            sizeMap.remove(row.head)
          }
        })
        // assume if sizemap is empty no point in proceceding
        if (sizeMap.isEmpty) {
          context.system.terminate
          println("no duplicates found")
        } else {
          // else let us do the firstRound of checksum calc. starting with tail
          // create the actor
          val firstRound = context.actorOf(Props(classOf[FirstRound], sizeMap, map))
          // send the message
          firstRound ! ReadTail
        }

      }

    case Finish =>

      sender ! PoisonPill
      subActors -= sender

      if (subActors.isEmpty) {

        // same concept as explained above
        // if you have only one row no point in keeping the file or scan it futther
        var y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
        y.keys.map(row => {
          if (row.size == 1) {
            map.remove(row.head)
          }
        })
      }


  }
}

// let us calculate the size first
class ScanAndCalculateSize(manager: ActorRef, dir: File, sizeMap: TrieMap[String, Long]) extends Actor {


  var counterForSelfActors = 0

  def receive: Receive = {
    //message for scan.. will get one for each directory
    // let us use this to send a message to calculate size
    case Scan =>
      val files = dir.listFiles()
      if (Option(files).isDefined && files.size > 0) {
        files.foreach(f => {
          if (f.isDirectory && !(f.getName == "." || f.getName == "..")) {
            // if directory again repeat it
            manager ! Recurse(f)
            manager ! SizeCalculationDone
          } else {
            if (f.canRead && f.length > 0) {
              counterForSelfActors += 1
              self ! CalculateSize(f)
            } else {
              manager ! SizeCalculationDone
            }
          }
        })
      } else {
        manager ! SizeCalculationDone
      }
    //message for size calculatio
    case CalculateSize(file) =>
      // create the size map
      val len = file.length()
      sizeMap.put(file.getAbsolutePath, len)
      counterForSelfActors -= 1
      if (counterForSelfActors == 0) {
        //send the message to master that you are done with size calc
        manager ! SizeCalculationDone
      }


  }

}

// helper object
object Capacity {
  //this I'm creating based on the sustem I haave..If I had more memey then this can easly increased
  def createCapacity(file: File) = {
    val length = file.length
    val capacity = length match {
      case x if x < 1000000l => length
      case x if x > 1000000l && x <= 10000000l => 1000000l
      case x if x > 10000000l && x <= 100000000l => 10000000l
      case x if x > 100000000l && x <= 1000000000l => 100000000l
      case x if x > 1000000000l => 1000000000l
    }
    capacity
  }
}

// calculate checkSum
class CheckSum(manager: ActorRef, file: File, map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]) extends Actor {
  def receive: Receive = {

    //this message is usef for poulating the base map
    case Calculate(buffer, future, channel, position) =>
      // get the future ojbect
      val currentPos = future.get()

      if (future.isDone) {
        buffer.flip()
        // using md5.. trried Adler32 too
        val md = MessageDigest.getInstance("MD5")
        md.update(buffer)
        val csValue = Base64.getEncoder.encodeToString(md.digest())
        md.reset()
        //        val cs = new Adler32
        //        cs.update(buffer)
        //      val csValue=cs.getValue.toString
        channel.close()

        var tuple2 = map.get(file.getAbsolutePath)
        var checkSumValue = ArrayBuffer[String]()
        if (tuple2.isDefined) {
          checkSumValue = tuple2.get._2
        }
        checkSumValue += (csValue)

        // if u reached end of file or start of file
        if (currentPos == -1 || currentPos == file.length || currentPos == 0) {
          tuple2 = Some(Tuple2(-1l, checkSumValue))
        } else {
          // else store it in tuple2 for subsquent rounds
          tuple2 = Some(Tuple2(position + buffer.capacity(), checkSumValue))
        }
        map.put(file.getAbsolutePath, tuple2.get)
        buffer.clear()
        manager ! Finish
      }

    //message used to read the channel
    case ReadFileChannel(fileChannel, position, capacity) =>
      //allocate the buffer
      val buffer = ByteBuffer.allocate(capacity.intValue)
      // read from the postion to the positon stated in buffer
      val future = fileChannel.read(buffer, position)
      // call calucate
      self ! Calculate(buffer, future, fileChannel, position)

  }
}

// for tail
class FirstRound(sizeMap: TrieMap[String, Long], map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]) extends Actor {

  //will be using this for checkSum actor
  var tailCheckSubs = new ArrayBuffer[ActorRef]

  def receive: Receive = {

    // messge this is use for reading tail
    case ReadTail =>

      //parse the sizemap and iterate the files and let ius now populate the other checksummap

      sizeMap.keys.map(fileString => {
        val file = new File(fileString)
        val sub = context.actorOf(Props(classOf[CheckSum], self, file, map))
        tailCheckSubs += sub
        // use the nio to create the channel
        val channel = AsynchronousFileChannel.open(file.toPath, StandardOpenOption.READ)
        val capacity = Capacity.createCapacity(file)
        // not here the postion is from tail
        sub ! ReadFileChannel(channel, file.length - capacity, capacity)
      })

    // message this is used when the actor is finised
    case Finish =>
      sender ! PoisonPill
      tailCheckSubs -= sender
      // once all the actors done
      // do the same things if there is only  1 row
      if (tailCheckSubs.isEmpty) {
        val y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
        y.keys.map(row => {
          if (row.size == 1) {
            map.remove(row.head)
          }
        })
        // just see how much we have
        println("after tail", map.size)
        // let us now do the head
        val secondRound = context.actorOf(Props(classOf[SecoundRound], map))
        secondRound ! ReadHead

      }


  }

}

//for head
class SecoundRound(map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]) extends Actor {

  //same concept
  var checkSumSubs = new ArrayBuffer[ActorRef]

  def receive: Receive = {

    // message received for reading head
    case ReadHead =>
      // iterate our basemap
      map.keys.map(fileString => {
        if (map.get(fileString).get._1 > 0) {
          // let us do the same logic of creating the checkSum
          val file = new File(fileString)
          val capacity=Capacity.createCapacity(file)
          val sub = context.actorOf(Props(classOf[CheckSum], self, file, map))
          checkSumSubs += sub
          val channel = AsynchronousFileChannel.open(file.toPath, StandardOpenOption.READ)
          // not here the postion is 0
          sub ! ReadFileChannel(channel, 0l, capacity)
        }else{
          self ! Finish
        }
      })

    case Finish =>
      // sender will no lenger send message
      if (checkSumSubs.contains(sender)) {
        sender ! PoisonPill
        checkSumSubs -= sender
      }

      // once all the senders are done

      if (checkSumSubs.isEmpty) {
        // remove if there are no duplicates
        var y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
        y.keys.map(row => {
          if (row.size == 1) {
            map.remove(row.head)
          }
        })

        // check if you have scanned the entire file
        val allFilesFinished = map.filterNot(_._2._1 == -1)

        if (map.isEmpty || allFilesFinished.isEmpty) {
          context.system.terminate
          // if all done voila.. print it
          if (allFilesFinished.isEmpty) {
            y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
            y.keys.map(row => {
              println(row)
            })
          }
        }
        // if not let us read the entire file for all checksums
        if (!allFilesFinished.isEmpty) {
          println("after head", map.size)
          val thirdRound = context.actorOf(Props(classOf[FinalRound], map))
          thirdRound ! ReadRemainingContentsOfTheFile

        }
      }

  }

}

// for remainder of the file contents
class FinalRound(map: TrieMap[String, Tuple2[Long, ArrayBuffer[String]]]) extends Actor {
  var checkSumSubs = new ArrayBuffer[ActorRef]

  def receive: Receive = {
    case ReadRemainingContentsOfTheFile =>

      val y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
      y.keys.foreach(row => {

        val tuple2 = y.get(row)
        val position = tuple2.get._1
        if (position > 0) {

          //same concept above.. however look the positon concept
          row.map(f => {
            val file = new File(f)
            val sub = context.actorOf(Props(classOf[CheckSum], self, file, map))
            checkSumSubs += sub
            val channel = AsynchronousFileChannel.open(file.toPath, StandardOpenOption.READ)
            val capacity = Capacity.createCapacity(file)
            // we take the position where we left off
            sub ! ReadFileChannel(channel, position, capacity)
          })
        }

      })

    case Finish =>


      sender ! PoisonPill
      checkSumSubs -= sender

      // once done
      if (checkSumSubs.isEmpty) {
        //remove if we have only singe value
        var y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
        y.keys.map(row => {
          if (row.size == 1) {
            map.remove(row.head)
          }
        })

        // check if all done
        val allFilesFinished = map.filterNot(_._2._1 == -1)

        // if all done
        if (map.isEmpty || allFilesFinished.isEmpty) {

          if (allFilesFinished.isEmpty) {
            println("after final",map.size)
            y = map.groupBy(_._2).mapValues(_.map(_._1)).map(_.swap)
            y.keys.map(row => {
              println(row)
            })
          }
          context.system.terminate

        }

        //if we havent finised reading all the file
        if (!allFilesFinished.isEmpty) {
          //repeat it
          self ! ReadRemainingContentsOfTheFile
        }
      }
  }
}

