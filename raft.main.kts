#!/usr/bin/env kotlin
// ./maelstrom test -w kafka --bin /Users/pravin/gossip_glomers/kafka.main.kts  --node-count 1 --concurrency 2n --time-limit 20  --rate 1000 --log-stderr
//cat store/latest/node-logs/n1.log
@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.PropertyWriter
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Condition
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
        val newNode = Node(echoMsg.dest, body.nodeIds?:emptyList())
        nodeMap.put(echoMsg.dest, newNode)
    }
    val node =  nodeMap.get(echoMsg.dest)
    System.err.println("Received $input")
     node?.sendReplyMsg(echoMsg)

}

class Node(val nodeId:String, val nodeIds:List<String>){
    var nodeState = "follower"
    private val lock = ReentrantLock()
    private val logLock = ReentrantLock()
    private val schedulerLock =   ReentrantLock()
    private val voteMsgLock =  ReentrantLock()
    private val appendEntryLock = ReentrantLock()
    private val operationLock = ReentrantLock()
    private val stepDownSchedulerLock =   ReentrantLock()
    private val logRepSchedulerLock =   ReentrantLock()
     private val candidateSchedulerLock =   ReentrantLock()
    var votedFor:String? = null
    val neighBorIds = nodeIds.minus(nodeId)
    var term = 0
    var electionDeadline = System.currentTimeMillis()
    val electionTimeout = 2000
    var stepDownDeadline = System.currentTimeMillis()
    var doesReadValueRec = true
    val entriesLog = listOf(LogEntry(0)).toMutableList()
    var voteResp = NodeMsg(0,"", MsgBody("error", code = 0),"")
    val  stateMachine = StorageMap()
    val minRepInterval = 50
    var lastReplicatedAt = System.currentTimeMillis()
    val nextIndexMap = mutableMapOf<String,Int>()
    val matchIndexMap = mutableMapOf<String,Int>()
    val heartBeatInterval = 1000
    var commitIndex = 0
    var lastAppliedIndex = 1
    val syncMsgMap =   mutableMapOf<Int,Boolean>()
    val srcMap =   mutableMapOf<Int,Pair<Int,String>>()
    val votes = mutableSetOf (nodeId)
    private var leaderNode = EMPTY_STRING
     private val voteMsgCondition =  voteMsgLock.newCondition()
     private val appendEntryCondition =  appendEntryLock.newCondition()
    private val operationCondition =  operationLock.newCondition()


    fun sendReplyMsg(echoMsg: NodeMsg){
        val body = echoMsg.body
        val randMsgId = (0..10000).random()
        val replyType = body.type+"_ok"
        val replyBody =  when(body.type){
            "init" -> {
                thread{
                    candidateScheduler()
                    stepDownScheduler()
                    logReplicateScheduler()
                }
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "request_vote_res" -> {
                voteMsgLock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                syncMsgMap.put(echoMsg.body.inReplyTo?:-1, true)
                voteResp = echoMsg
                voteMsgCondition.signal()
                voteMsgLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )

            }
            "request_vote" -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
              val shouldGrantVote =   respondToVoteRequest(body)
                logLock.unlock()
                MsgBody("request_vote_res", term = term,voteGranted = shouldGrantVote, inReplyTo = body.msgId )

            }
            in listOf("cas","read","write") ->{
                logLock.tryLock(5,TimeUnit.SECONDS)
                thread{ handleOperation(echoMsg)}
                logLock.unlock()
                MsgBody(replyType, msgId = randMsgId, inReplyTo = body.msgId )

            }
            in listOf("cas_ok", "read_ok","write_ok","error")->{
                operationLock.tryLock(5,TimeUnit.SECONDS)
               // doesReadValueRec = true
              //  syncMsgMap.put(echoMsg.body.inReplyTo?:-1, true)
                sendOpRespToClient(echoMsg)
              //  voteResp = echoMsg
              // operationCondition.signal()
                operationLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
            "append_entries" -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
                val replyBody = handleAppendEntries(body,body.msgId?:-1)
                logLock.unlock()
               replyBody
            }

            "append_entries_res" -> {
                appendEntryLock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                syncMsgMap.put(echoMsg.body.inReplyTo?:-1, true)
                voteResp = echoMsg
                appendEntryCondition.signal()
                appendEntryLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }




            else -> {
                System.err.println("In else ${body.type} message  recived")
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
        }
        if(body.type in listOf("read_ok","write_ok","cas_ok", "request_vote_res","cas","read","write", "write_ok", "error","append_entries_ok", "append_entries_res_ok")) return
        val msg = NodeMsg(echoMsg.id,echoMsg.src,replyBody,echoMsg.dest)
        val replyStr =   serializeMsg(msg)
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()

    }

    fun handleAppendEntries(body:MsgBody,msgId:Int):MsgBody{
       maybeStepDown(body.term?:-1)
        val replyBody = MsgBody("append_entries_res", term = term, success = false, inReplyTo = msgId)
        if(body.term?:-1 < term){
            System.err.println("Rejecting append entries Remote term :${body.term}, Node term: ${term}")
            return replyBody
        }
        val prevLogIndex = body.prevLogIndex?:-1
        leaderNode = body.leaderId?:EMPTY_STRING
        resetElectionDeadline()
        if( prevLogIndex <= 0)  {
            System.err.println("Out of bounds previous log index ${prevLogIndex}")
            return replyBody
        }
       val nodePrevLog =  entriesLog.getOrNull(prevLogIndex-1)
        if(nodePrevLog == null || nodePrevLog.term != body.prevLogTerm){
            System.err.println("Rejecting append entries NodePrevLog:${nodePrevLog}, RemotePrevLogTerm:${body.prevLogTerm}")
            return replyBody
        }
        entriesLog.subList(prevLogIndex, entriesLog.size).clear()
        entriesLog.addAll(body.entries?: emptyList())
        if(commitIndex < body.leaderCommit?:0){
            commitIndex = minOf(entriesLog.size, body.leaderCommit?:0)
            advanceStateMachine()
        }
        return MsgBody("append_entries_res", term = term, success = true, inReplyTo = msgId)

    }
    fun handleOperation(echoMsg:NodeMsg){
        val body = echoMsg.body
        val randMsgId = (0..10000).random()
        System.err.println("In Handle operation LeaderNode:${leaderNode}")
        if(nodeState == "leader"){
            entriesLog.add(LogEntry(term, echoMsg))
        }
        else if(leaderNode != EMPTY_STRING){
           val newBody =  body.copy()
            newBody.msgId = randMsgId
            val nodeMsg = NodeMsg(1,leaderNode, newBody, nodeId)
            srcMap.put(randMsgId,Pair(body.msgId?:-1,echoMsg.src))
            sendMsg(nodeMsg)
    }
    fun sendOpRespToClient(echoMsg:NodeMsg){
        val srcDetail = srcMap.get(echoMsg.body.inReplyTo)
        val replyBody = echoMsg.body.copy()
        replyBody.inReplyTo = srcDetail?.first
        val msg = NodeMsg(1,srcDetail?.second?:EMPTY_STRING,replyBody, nodeId  )
        System.err.println("sending msg to client : ${mapper.writeValueAsString(msg)}")
        sendMsg(msg)
    }
    fun becomeCandidate(){
        nodeState = "candidate"
        leaderNode = EMPTY_STRING
        advanceTerm(term+1)
        resetElectionDeadline()
        resetStepDownDeadline()
        System.err.println("Became candidate for term :${term}")
        votedFor = nodeId
        sendVoteReq()

    }


    fun advanceTerm(newTerm:Int){
        if(term > newTerm){
            System.err.println("Term can't go backwards")
        }
        else {
            term = newTerm
            votedFor = null
        }

    }

    fun advanceCommitIndex(){
        if(nodeState == "leader"){
            val medianCommit = getMedian(matchIndexMap.values.toList())
            if(commitIndex < medianCommit && entriesLog.get(medianCommit-1).term == term){
                System.err.println("Commit index now ${medianCommit}")
                commitIndex = medianCommit
            }
            advanceStateMachine()
        }

    }
    fun advanceStateMachine(){
        while(lastAppliedIndex < commitIndex){
            lastAppliedIndex += 1
            val logEntry = entriesLog.get(lastAppliedIndex-1).op
            System.err.println("Applying logEntry : ${mapper.writeValueAsString(logEntry)}")
            val resp =   stateMachine.apply(logEntry?.body?: MsgBody("error"))
            System.err.println("State machine resp is resp : ${resp}")
            val randMsgId = (0..10000).random()
            val replyBody =  MsgBody(resp.type, msgId = randMsgId, inReplyTo = logEntry?.body?.msgId ,value = resp.value, code = resp.code)
            val msg = NodeMsg(0, logEntry?.src?:EMPTY_STRING,replyBody,nodeId)
            if(nodeState == "leader"){
                System.err.println("sending msg after logEntry : ${mapper.writeValueAsString(msg)}")
                sendMsg(msg)
            }
        }

    }

    fun candidateScheduler(){
        val randInterval = (0..200).random().toLong()
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                candidateSchedulerLock.tryLock(5, TimeUnit.SECONDS)
                if(electionDeadline < System.currentTimeMillis())
                    if(nodeState != "leader" ){
                         becomeCandidate()
                    } else {
                        resetElectionDeadline()
                    }
                 candidateSchedulerLock.unlock()
            }

        }, 0+randInterval, 200+randInterval)

    }
    fun resetElectionDeadline(){
        electionDeadline = System.currentTimeMillis() +  (electionTimeout*(Math.random()+1)).toLong()
    }
    fun becomeFollower(){
        nodeState = "follower"
        leaderNode = EMPTY_STRING
        resetElectionDeadline()
        matchIndexMap.clear()
        nextIndexMap.clear()
        System.err.println("Became follower for term :${term}")
    }
    fun getTimeElapsed(startTime:Long):Long{
        return System.currentTimeMillis() - startTime
    }
    fun sendSyncMsg(msg:NodeMsg, syncMsgLock: ReentrantLock, condition: Condition):NodeMsg{
        syncMsgLock.tryLock(5,TimeUnit.SECONDS)
        sendMsg(msg)
        doesReadValueRec = false
        syncMsgMap.put(msg.body.msgId?:-1, false)
        while(!(syncMsgMap.get(msg.body.msgId?:-1)?:false) ){
            condition.await(50, TimeUnit.MILLISECONDS)
        }
        syncMsgLock.unlock()
        return voteResp

    }
    fun sendMsg(msg:NodeMsg){
        val msgStr  = serializeMsg(msg)
        System.err.println("Sent $msgStr")
        System.out.println( msgStr)
        System.out.flush()

    }
    fun sendVoteReq(){
        thread{
            val list = listOf(1,2)

            neighBorIds.map{
                val randMsgId = (0..10000).random()
                val termForVoteRequested = term
                   System.err.println("Before sending requeste_vote Log: ${entriesLog}")
                val replyBody = MsgBody(type = "request_vote",msgId = randMsgId, term = termForVoteRequested, candidateId = nodeId,lastLogIndex = entriesLog.size,lastLogTerm = entriesLog.last().term )
                val msg = NodeMsg(1, it, replyBody, nodeId)
                val msgResp =   sendSyncMsg(msg, voteMsgLock, voteMsgCondition)
                val body = msgResp.body
                val respTerm = body.term
                val voteGranted = body.voteGranted?:false
                maybeStepDown(respTerm?:0)
                System.err.println("Vote Resp Processing NS:${nodeState}, TERM:${term},RT:${respTerm} TVR: ${termForVoteRequested}, VG:${voteGranted}")
                if(nodeState == "candidate" && term == respTerm && term == termForVoteRequested && voteGranted ){
                    votes.add(msgResp.src)
                    System.err.println("Have votes :${votes}")
                    val majorityNo = getMajorityNumber()
                    if(majorityNo <= votes.size) becomeLeader()
                }


            }
        }
        resetStepDownDeadline()


    }
    fun maybeStepDown(remoteTerm:Int){
        if(term < remoteTerm){
            System.err.println("Stepping Down: remoteTerm: ${remoteTerm} Node term: ${term}")
            advanceTerm(remoteTerm)
            becomeFollower()
        }


    }
    fun getMajorityNumber():Int{
        val nodeSize = nodeIds.size
        return (nodeSize.floorDiv(2)) + 1

    }
    fun becomeLeader(){
        lock.tryLock(5,TimeUnit.SECONDS)
        if(nodeState != "candidate"){
            System.err.println("Should be a candidate")
            return
        }
        leaderNode = EMPTY_STRING
        nodeState = "leader"
        lastReplicatedAt = 0
        nextIndexMap.clear()
        matchIndexMap.clear()
        neighBorIds.map{
            nextIndexMap.put(it, entriesLog.size+1)
            matchIndexMap.put(it,0)
        }
        resetStepDownDeadline()
        System.err.println("Became leader for term :${term}")
        lock.unlock()

    }

    fun resetStepDownDeadline(){
        lock.tryLock(5,TimeUnit.SECONDS)
        stepDownDeadline = System.currentTimeMillis() +  (electionTimeout*(Math.random()+1)).toLong()
        lock.unlock()

    }
    fun respondToVoteRequest(body:MsgBody):Boolean{
        var grantVote = false
        maybeStepDown(body.term?:0)
        if(body.term?:0 < term) {
            System.err.println("Candidate term ${body.term} lower than ${term}, not granting vote.")
        }
        else if(votedFor != null){
            System.err.println("Already voted for ${votedFor}, not granting vote")
        }
        else if(body.lastLogTerm?:0 < entriesLog.last().term){
            System.err.println("Have log entries from term ${entriesLog.last().term} which is newer than remote term  ${body.lastLogTerm}, not granting vote")
        }
        else if(body.lastLogTerm == entriesLog.last().term && body.lastLogIndex?:0 < entriesLog.size){
            System.err.println("Our logs are both at term ${body.lastLogTerm} but our log is ${entriesLog.size} and their is ${body.lastLogIndex} long, not granting vote")
        }
        else {
            System.err.println("Granting vote to ${body.candidateId}")
            grantVote = true
            votedFor = body.candidateId
            resetElectionDeadline()
        }
      return grantVote

    }
    fun stepDownScheduler(){
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                stepDownSchedulerLock.tryLock(5,TimeUnit.SECONDS)
                if(stepDownDeadline < System.currentTimeMillis() && nodeState == "leader"){
                    System.err.println("Stepping down: haven't received any acks recently")
                    becomeFollower()
                }
                 stepDownSchedulerLock.unlock()


            }
        }, 0, 100)
    }

    fun replicateLog(){
        val elapsedTime = System.currentTimeMillis() - lastReplicatedAt
        var replicated = false
        val term = term
        if(nodeState == "leader")  System.err.println("Replicate log called ET: ${elapsedTime}")
        if (nodeState == "leader" && minRepInterval < elapsedTime){
            neighBorIds.mapIndexed{index,neighBorId->
                val ni =  nextIndexMap.get(neighBorId)?:0
                val prevLogTerm = when(ni >= 2){
                    true -> entriesLog[ni-2].term
                    else -> -1
                }
                 System.err.println("Next index before slice:${ni},NID:${neighBorId}, NIM:${nextIndexMap},NS:${nodeState}, ID:${index}")
                val entriesToReplicate = entriesLog.slice(ni-1..entriesLog.size-1)
                if(entriesToReplicate.size > 0 || heartBeatInterval < elapsedTime){
                    System.err.println("Replicating ${ni} to ${neighBorId}")
                    replicated = true
                    val randMsgId = (0..10000).random()
                    val body = MsgBody("append_entries", leaderCommit = commitIndex, msgId = randMsgId, term = term,leaderId = nodeId,prevLogIndex = ni-1, prevLogTerm = prevLogTerm, entries = entriesToReplicate )
                    val nodeMsg = NodeMsg(1,neighBorId,body,nodeId)
                    val msgResp = sendSyncMsg(nodeMsg,appendEntryLock,appendEntryCondition)
                    val replyBody = msgResp.body
                    System.err.println("After append entries Resp: NS :${nodeState}, RT: ${replyBody.term}, TERM: ${term}")
                    maybeStepDown(replyBody.term?:-1)
                    if(nodeState == "leader" && replyBody.term == term){
                        resetStepDownDeadline()
                        if(replyBody.success?:false){
                            nextIndexMap.put(neighBorId, maxOf(nextIndexMap.get(neighBorId)?:0, ni.plus(entriesToReplicate.size)))
                            matchIndexMap.put(neighBorId, maxOf(matchIndexMap.get(neighBorId)?:0, ni.plus(entriesToReplicate.size -1)))
                            advanceCommitIndex()
                        }
                        else{
                         val newNextIndex =   maxOf((nextIndexMap.get(neighBorId)?:0)-1,2 )
                         nextIndexMap.put(neighBorId,newNextIndex)
                        }
                    }

                }
            }
            if(replicated) lastReplicatedAt = System.currentTimeMillis()
        }

    }

    fun logReplicateScheduler(){
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                logRepSchedulerLock.tryLock(5,TimeUnit.SECONDS)
                replicateLog()
                logRepSchedulerLock.unlock()
            }
        }, 0, minRepInterval.toLong())
    }

    fun serializeMsg(msg:NodeMsg):String{
        return mapper.writeValueAsString(msg)
    }

    fun getMedian(list:List<Int>):Int{
       val majorityNumber = getMajorityNumber()
      return  list.sorted().get(list.size - majorityNumber)
    }





}

class StorageMap(){
    val storageMap = mutableMapOf<String,Int>()
    fun apply(op:MsgBody):OpResult{
        val key = op.key?:""

        val result =   when(op.type){

            "read" -> {
                val value = storageMap.get(key)
                if(value != null){
                    OpResult("read_ok",value =  value)
                }
                else  OpResult("read_ok",msg = "key not found", value = value)
            }
            "write" -> {
                storageMap.put(key, value = op.value?:-1)
                OpResult("write_ok")
            }
            "cas" -> {
                val value = storageMap.get(key)
                if(value != null) {
                    if(value != op.from) OpResult("error",msg = "expected ${op.from}, but had ${value}",code = 22)
                    else {
                        storageMap.put(key, op.to?:-1)
                        OpResult("cas_ok")
                    }

                }
                else  OpResult("error",msg = "key not found", code = 20)

            }
            else  -> {OpResult("error",msg =  "error msg")}
        }
        return result

    }
}



data class NodeMsg(
    val id:Int,
    val dest:String,
    @JsonSerialize(using = MyClassSerializer::class)
    val body:MsgBody,
    val src:String

)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MsgBody(
    val type:String,
    @JsonProperty("node_id") val nodeId:String? = null,
    @JsonProperty("node_ids")  val nodeIds:List<String>? = null,
    @JsonProperty("msg_id")   var msgId:Int? = null,
    @JsonProperty("in_reply_to")   var inReplyTo :Int? = null,
    var term:Int? = null,
    @JsonProperty("last_log_index") val lastLogIndex:Int? = null,
    @JsonProperty("last_log_term") val lastLogTerm:Int? = null,
    @JsonProperty("vote_granted") val voteGranted:Boolean? = null,
    @JsonProperty("candidate_id") val candidateId:String? = null,
    val key:String? = null,
    val from: Int? = null,
    val to: Int? = null,
     val  value:Int? = null,
    @JsonProperty("leader_id") val leaderId:String? = null,
    @JsonProperty("prev_log_index") val prevLogIndex:Int? = null,
    @JsonProperty("prev_log_term") val prevLogTerm:Int? = null,
    var success:Boolean? = null,
    val entries:List<LogEntry>? = null,
    @JsonProperty("leader_commit") val leaderCommit:Int? = null,
    val code:Int? = null

    )

class MyClassSerializer : JsonSerializer<MsgBody>() {
    override fun serialize(body: MsgBody, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeStartObject()

        // Use the default serializer for all properties except `value`
        val defaultSerializer = serializers.findValueSerializer(MsgBody::class.java)
        defaultSerializer.unwrappingSerializer(null).serialize(body, gen, serializers)

        // Serialize `value` only if `type` is "read_ok"
        if (body.type == "read_ok" && body.value == null) {
            gen.writeNullField("value")
        }

        gen.writeEndObject()
    }
}


data class LogEntry(
    val term:Int,
    val op:NodeMsg?=null
)


data class OpResult(
    val type:String,
    val msg:String? = null,
    val value:Int? = null,
    val code:Int? = null
)






