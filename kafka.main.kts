#!/usr/bin/env kotlin
// ./maelstrom test -w kafka --bin /Users/pravin/gossip_glomers/kafka.main.kts  --node-count 1 --concurrency 2n --time-limit 20  --rate 1000 --log-stderr
@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread


val mapper = jacksonObjectMapper()
val EMPTY_STRING = ""

val nodeMap = mutableMapOf<String,Node>()

while(true){
    val input = readLine()
    val echoMsg = mapper.readValue(input, NodeMsg::class.java)
    val body = echoMsg.body
    if(body.type == "init"){
        val newNode = Node(echoMsg.dest, emptyList(),0)
        nodeMap.put(echoMsg.dest, newNode)
    }
    val node =  nodeMap.get(echoMsg.dest)
    System.err.println("Received $input")
   thread{ node?.sendReplyMsg(echoMsg)}

}

class Node(
        val nodeId:String,
        val nodeIds:List<String>,
        val nextMsgId:Int
) {
    private val lock = ReentrantLock()
    private val logLock = ReentrantLock()
    private val mapper = jacksonObjectMapper()
    private var doesValueRec = false
    private val condition = lock.newCondition()
    private var linKVStoreValue:Any? = null
    private var wasWriteSuccessful = false

    fun logMsg(msg:String) {
        logLock.tryLock(5, TimeUnit.SECONDS)
        System.err.println(msg)
        System.out.flush()
        logLock.unlock()
    }


    fun sendReplyMsg(echoMsg: NodeMsg){
        val body = echoMsg.body
        val randMsgId = (0..10000).random()
        val replyType = body.type+"_ok"
        val replyBody =  when(body.type){
            "init" -> {
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "send" -> {
                val key = body.key?:EMPTY_STRING
                val offset =  writeToKey(key, body.msg?:-1)
                val replyTypeForSend = when(offset>=0){
                    true -> replyType
                    else -> "error"
                }
                MsgBody(replyTypeForSend,msgId = randMsgId, inReplyTo = body.msgId , offset = offset )
            }

            "poll" -> {
                val pollOffsets = body.offsets
                val logMsgs = mutableMapOf<String, List<List<Int>>>()
                logLock.tryLock(5, TimeUnit.SECONDS)
                pollOffsets?.forEach {
                    val logKey = it.key
                    val logOffset = it.value
                    val kafkaLogValue = try {readValueForKey(logKey) as List<List<Int>>}
                    catch (e:java.lang.Exception){
                        emptyList()
                    }

                  val valueToBeSent = kafkaLogValue.map{ valueOffSetList->
                      val index = valueOffSetList.firstOrNull()?:-1
                      val value = valueOffSetList.get(1)
                      if(index >= logOffset){
                          listOf(index, value)
                      } else null

                  }.filterNotNull()

                    logMsgs.put(logKey, valueToBeSent)

                }
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, msgs = logMsgs )
            }

            "commit_offsets" -> {
                logLock.tryLock(5, TimeUnit.SECONDS)
                body.offsets?.map{
                    overWriteKey("co_${it.key}", it.value)
                }
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "list_committed_offsets" -> {
                val offsets = mutableMapOf<String,Int>()
                body.keys?.map{
                    val linKVOffset = try{readValueForKey("co_${it}") as Int}
                    catch(e:java.lang.Exception){
                        -1
                    }
                  offsets.put(it, linKVOffset)
                }
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, offsets = offsets )

            }

            "read_ok" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                val storeValue = body.value
                linKVStoreValue = storeValue
                doesValueRec = true
                condition.signal()
                lock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "cas_ok", "write_ok" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesValueRec = true
                wasWriteSuccessful = true
                condition.signal()
                lock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "error" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesValueRec = true
                linKVStoreValue = null
                condition.signal()
                lock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }



            else -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
                System.err.println("In else ${body.type} message  recived")
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
        }
        if(body.type in listOf("read_ok","write_ok", "cas_ok", "write_ok", "error")) return
        val msg = NodeMsg(echoMsg.id,echoMsg.src,replyBody,echoMsg.dest)
        val replyStr =   mapper.writeValueAsString(msg)
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()


    }
    fun sendMsgSync(destId:String, body:MsgBody):Any?{
        lock.tryLock(5,TimeUnit.SECONDS)
        val msgToBeSent =  NodeMsg(1,destId, body, nodeId)
        val replyStr =   mapper.writeValueAsString(msgToBeSent)
        System.err.println("Sent to lin-kv $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        doesValueRec = false
        while(!doesValueRec){
            condition.await()
        }
        lock.unlock()
        return linKVStoreValue

    }

    fun readValueForKey(key:String):Any?{
        val randMsgId = (0..10000).random()
        val body = MsgBody("read",msgId = randMsgId, key = key)
        val linkvValue = sendMsgSync("lin-kv", body)
        return linkvValue
    }

    fun writeToKey(key:String, value:Int):Int{
        val randMsgId = (0..10000).random()
        val body = MsgBody("read",msgId = randMsgId, key = key)
        val linkvValue = try{ sendMsgSync("lin-kv", body) as List<List<Int>>}
        catch (e:Exception){
         emptyList()
        }
        val newValue =   linkvValue.toMutableList().plus(listOf(listOf(linkvValue.size, value)))
        val writeReq = MsgBody("cas", key = key, from = linkvValue, to = newValue, createIfNotExists = true ,msgId = (0..10000).random() )
        logLock.tryLock(5,TimeUnit.SECONDS)
         wasWriteSuccessful = false
         sendMsgSync("lin-kv", writeReq)
        logLock.unlock()
        return when(wasWriteSuccessful){
            true -> newValue.size -1
            else -> -1
        }
    }

    fun overWriteKey(key:String, value: Int):Boolean{
        val writeReq = MsgBody("write", key = key,  value = value ,msgId = (0..10000).random() )
        logLock.tryLock(5,TimeUnit.SECONDS)
        wasWriteSuccessful = false
        sendMsgSync("lin-kv", writeReq)
        logLock.unlock()
        return wasWriteSuccessful

    }

}





data class NodeMsg(
        val id:Int,
        val dest:String,
        val body:MsgBody,
        val src:String

)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MsgBody(
        val type:String,
        @JsonProperty("node_id") val nodeId:String? = null,
        @JsonProperty("node_ids")  val nodeIds:List<String>? = null,
        @JsonProperty("msg_id")   val msgId:Int,
        @JsonProperty("in_reply_to")   val inReplyTo :Int? = null,
        val key:String? = null,
        val msg:Int? = null,
        val offset:Int? = null,
        val offsets:Map<String, Int>? = null,
        val msgs:Map<String,List<List<Int>>>? = null,
        val keys:List<String>? = null,
        val value:Any? = null,
        @JsonProperty("create_if_not_exists")   val createIfNotExists :Boolean? = null,
        val from:List<List<Int>>? = null,
        val to:List<List<Int>>? = null,
        val code:Int? = null,
        val text:String? = null

)