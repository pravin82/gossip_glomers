#!/usr/bin/env kotlin
// ./maelstrom test -w broadcast  --bin /Users/pravin/script2/script2.main.kts   --time-limit 5  --log-stderr
@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock


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
    node?.sendReplyMsg(echoMsg)

}

class Node(
        val nodeId:String,
        val nodeIds:List<String>,
        val nextMsgId:Int
) {
    private val lock = ReentrantLock()
    private val logLock = ReentrantLock()
    private val mapper = jacksonObjectMapper()
    private val kafkaLogs = mutableMapOf<String, List<Int>>()
    private val commitedOffsets = mutableMapOf<String, Int>()


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
                val logsList = kafkaLogs.get(key)?.toMutableList()
                val newList = (logsList?: emptyList()).plus(body.msg?:-1)
                logLock.tryLock(5, TimeUnit.SECONDS)
                 kafkaLogs.put(key, newList )
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId , offset = newList.size -1 )
            }

            "poll" -> {
                val pollOffsets = body.offsets
                val logMsgs = mutableMapOf<String, List<List<Int>>>()
                logLock.tryLock(5, TimeUnit.SECONDS)
                pollOffsets?.forEach {
                    val logKey = it.key
                    val logOffset = it.value
                    val kafkaLogValue = kafkaLogs.get(logKey)

                  val valueToBeSent =   kafkaLogValue?.mapIndexed{index, i ->
                     if(index >= logOffset){
                         listOf(index, i)
                     } else null

                    }?.filterNotNull()

                    logMsgs.put(logKey, valueToBeSent?: emptyList())

                }
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, msgs = logMsgs )
            }

            "commit_offsets" -> {
                logLock.tryLock(5, TimeUnit.SECONDS)
                body.offsets?.map{
                    commitedOffsets.put(it.key, it.value)
                }
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "list_committed_offsets" -> {
                val offsets = mutableMapOf<String,Int>()
                body.keys?.map{
                  offsets.put(it, commitedOffsets.get(it)?:-1)
                }
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId, offsets = offsets )

            }



            else -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
                System.err.println("In else ${body.type} message  recived")
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
        }

        val msg = NodeMsg(echoMsg.id,echoMsg.src,replyBody,echoMsg.dest)
        val replyStr =   mapper.writeValueAsString(msg)
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()


    }
    //MsgId will be -1 if it is being sent from node
    fun sendMsg(destId:String, msg:NodeMsg){
        val body = msg.body
        val bodyToBeSent = MsgBody(body.type,msgId = (0..10000).random(), inReplyTo = body.msgId)
        val msgToBeSent =  NodeMsg(msg.id,destId, bodyToBeSent, nodeId)
        val replyStr =   mapper.writeValueAsString(msgToBeSent)
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent to Neighbor $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()

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
        val keys:List<String>? = null

)