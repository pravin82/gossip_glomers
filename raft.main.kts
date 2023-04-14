#!/usr/bin/env kotlin
// ./maelstrom test -w kafka --bin /Users/pravin/gossip_glomers/kafka.main.kts  --node-count 1 --concurrency 2n --time-limit 20  --rate 1000 --log-stderr
@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*
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
    var votedFor:String? = null
    val neighBorIds = nodeIds.minus(nodeId)
    var term = 0
    var electionDeadline = System.currentTimeMillis()
    val electionTimeout = 2000
    var stepDownDeadline = System.currentTimeMillis()
    var doesReadValueRec = true
    val entriesLog = listOf(LogEntry(0)).toMutableList()
    var voteResp = NodeMsg(0,"", MsgBody(""),"")
    val  stateMachine = StorageMap()
    val minRepInterval = 50



    private val condition = lock.newCondition()
    val votes = mutableSetOf (nodeId)


    fun sendReplyMsg(echoMsg: NodeMsg){
        val body = echoMsg.body
        val randMsgId = (0..10000).random()
        val replyType = body.type+"_ok"
        val replyBody =  when(body.type){
            "init" -> {
                thread{
                    candidateScheduler()
                    stepDownScheduler()
                }
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }

            "request_vote_res" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                voteResp = echoMsg
                condition.signal()
                lock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )

            }
            "request_vote" -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
              val shouldGrantVote =   respondToVoteRequest(body)
                logLock.unlock()
                MsgBody("request_vote_res", term = term,voteGranted = shouldGrantVote )

            }
            in listOf("cas","read","write") ->{
                logLock.tryLock(5,TimeUnit.SECONDS)
               val opResult = handleOperation(body)
                logLock.unlock()
                MsgBody(opResult.type, msgId = randMsgId, inReplyTo = body.msgId ,value = opResult.value)

            }


            else -> {
                logLock.tryLock(5,TimeUnit.SECONDS)
                System.err.println("In else ${body.type} message  recived")
                logLock.unlock()
                MsgBody(replyType,msgId = randMsgId, inReplyTo = body.msgId )
            }
        }
        if(body.type in listOf("read_ok","write_ok", "request_vote_res", "write_ok", "error")) return
        val msg = NodeMsg(echoMsg.id,echoMsg.src,replyBody,echoMsg.dest)
        val replyStr =   mapper.writeValueAsString(msg)
        lock.tryLock(5,TimeUnit.SECONDS)
        System.err.println("Sent $replyStr")
        System.out.println( replyStr)
        System.out.flush()
        lock.unlock()

    }
    fun handleOperation(body:MsgBody):OpResult{
        var opResult = OpResult("error", msg = "not a leader")
        if(nodeState == "leader"){
            entriesLog.add(LogEntry(body.term?:0, body))
            opResult =  stateMachine.apply(body)
            System.err.println("Log of leader :${mapper.writeValueAsString(entriesLog)}")
        }
        return opResult

    }
    fun becomeCandidate(){
        logLock.tryLock(5,TimeUnit.SECONDS)
        nodeState = "candidate"
        advanceTerm(term+1)
        resetElectionDeadline()
        resetStepDownDeadline()
        System.err.println("Became candidate for term :${term}")
        votedFor = nodeId
        sendVoteReq()
        logLock.unlock()

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

    fun candidateScheduler(){
        val randInterval = (0..100).random().toLong()
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                if(electionDeadline < System.currentTimeMillis())
                    if(nodeState != "leader" ){
                        becomeCandidate()
                    } else {
                        resetElectionDeadline()
                    }

            }
        }, 0+randInterval, 100+randInterval)

    }
    fun resetElectionDeadline(){
        lock.tryLock(5,TimeUnit.SECONDS)
        electionDeadline = System.currentTimeMillis() +  (electionTimeout*(Math.random()+1)).toLong()
        lock.unlock()
    }
    fun becomeFollower(){
        lock.tryLock(5,TimeUnit.SECONDS)
        nodeState = "follower"
        resetElectionDeadline()
        System.err.println("Became follower for term :${term}")
        lock.unlock()

    }
    fun sendSyncMsg(msg:String):NodeMsg{
        lock.tryLock(5,TimeUnit.SECONDS)
        System.out.println( msg)
        System.out.flush()
        doesReadValueRec = false
        while(!doesReadValueRec){
            condition.await()
        }
        lock.unlock()
        return voteResp

    }
    fun sendVoteReq(){
        thread{
            neighBorIds.map{
                val randMsgId = (0..10000).random()
                val termForVoteRequested = term
                val replyBody = MsgBody(type = "request_vote", term = termForVoteRequested, candidateId = nodeId,lastLogIndex = entriesLog.size,lastLogTerm = entriesLog.last().term )
                val msg = NodeMsg(randMsgId, it, replyBody, nodeId)
                val msgStr = mapper.writeValueAsString(msg)
                System.err.println("Vote Req Msg Sent: ${msgStr}")
                val msgResp =   sendSyncMsg(msgStr)
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
        lock.tryLock(5,TimeUnit.SECONDS)
        if(term < remoteTerm){
            System.err.println("Stepping Down: remoteTerm: ${remoteTerm} Node term: ${term}")
            advanceTerm(remoteTerm)
            becomeFollower()
        }
        lock.unlock()


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
        nodeState = "leader"
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
                if(electionDeadline < System.currentTimeMillis() && nodeState == "leader"){
                    System.err.println("Stepping down: haven't received any acks recently")
                    becomeFollower()
                }


            }
        }, 0, 100)

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
                else  OpResult("error",msg = "key not found")
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
    val body:MsgBody,
    val src:String

)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MsgBody(
    val type:String,
    @JsonProperty("node_id") val nodeId:String? = null,
    @JsonProperty("node_ids")  val nodeIds:List<String>? = null,
    @JsonProperty("msg_id")   val msgId:Int? = null,
    @JsonProperty("in_reply_to")   val inReplyTo :Int? = null,
    var term:Int? = null,
    @JsonProperty("last_log_index") val lastLogIndex:Int? = null,
    @JsonProperty("last_log_term") val lastLogTerm:Int? = null,
    @JsonProperty("vote_granted") val voteGranted:Boolean? = null,
    @JsonProperty("candidate_id") val candidateId:String? = null,
    val key:String? = null,
    val from: Int? = null,
    val to: Int? = null,
    val value:Int? = null,

    )

data class LogEntry(
    val term:Int,
    val body:MsgBody?=null
)


data class OpResult(
    val type:String,
    val msg:String? = null,
    val value:Int? = null,
    val code:Int? = null
)

