#!/usr/bin/env kotlin

@file:Repository("https://jcenter.bintray.com")
@file:DependsOn("com.fasterxml.jackson.core:jackson-core:2.14.2")
@file:DependsOn("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
@file:Import("dtos.main.kts")

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.concurrent.thread





class StorageMap(){
    val storageMap = mutableMapOf<String,Int>()
    fun apply(op:EchoBody):OpResult{
        val key = op.key?:""
        val x = y

        val result =   when(op.type){

            "read" -> {
                val value = storageMap.get(key)
                if(value != null){
                    OpResult("read_ok", value)
                }
                else  OpResult("error",msg = "key not found")
            }
            "write" -> {
                storageMap.put(key, op.value?:-1)
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

class Raft(val nodeId:String,val nodeIds:List<String>){
    val stateMachine = StorageMap()
    var candidateState = "follower"
    var electionDeadline = System.currentTimeMillis()
    val electionTimeout = 2000
    var stepDownDeadline = System.currentTimeMillis()
    var term = 0
    val lock = ReentrantLock()
    val neighBorIds = nodeIds.minus(nodeId)
    var voteResp = EchoMsg(0,"", EchoBody(""),"")
    val stateLock = ReentrantLock()
    private val condition = lock.newCondition()
    val entriesLog = listOf(LogEntry(0)).toMutableList()
    val mapper = jacksonObjectMapper()
    var doesReadValueRec = true
    var votedFor:String? = null
    val votes = mutableSetOf<String> (nodeId)
    var commitIndex = 0
    val nextIndexMap = mutableMapOf<String,Int>()
    val matchIndexMap = mutableMapOf<String,Int>()
    val heartBeatInterval = 1000
    val minRepInterval = 50
    var lastReplication = System.currentTimeMillis()

    fun handleClientReq(msg:EchoMsg):EchoBody{
        val body = msg.body
        when(body.type){
            "request_vote_res" -> {
                lock.tryLock(5,TimeUnit.SECONDS)
                doesReadValueRec = true
                voteResp = msg
                condition.signal()
                lock.unlock()

            }
            "request_vote" -> {
                respondToVoteRequest(body)
            }
            "append_entries" -> {
                handleAppendEntriesReq(body)
            }

        }
        lock.tryLock(5,TimeUnit.SECONDS)
        val randMsgId = (0..10000).random()
        var opResult = OpResult("error", msg = "not a leader")
        if(candidateState == "leader" && body.type in listOf("cas", "read", "write")){
            entriesLog.add(LogEntry(body.term?:0, body))
            opResult =  stateMachine.apply(body)
            System.err.println("Log of leader :${entriesLog}")
        }
        lock.unlock()
        val  replyBody =  EchoBody(opResult.type,msgId = randMsgId, inReplyTo = body.msgId, value = opResult.value )
        return replyBody
    }


    fun becomeCandidate(){
        lock.tryLock(5,TimeUnit.SECONDS)
        candidateState = "candidate"
        advanceTerm(term+1)
        resetElectionDeadline()
        resetStepDownDeadline()
        System.err.println("Became candidate for term :${term}")
        votedFor = nodeId
        sendVoteReq()
        lock.unlock()

    }
    fun getMajorityNumber():Int{
        val nodeSize = nodeIds.size
        return (nodeSize.floorDiv(2)) + 1

    }

    fun handleAppendEntriesReq(body:EchoBody){
        lock.tryLock(5,TimeUnit.SECONDS)
        maybeStepDown(body.term?:0)
        var replyBody = EchoBody(type = "append_entries_res", term = term, success = false)
        if(body.term >= term) {
            resetElectionDeadline()
            val prevLogIndex = body.prevLogIndex
            if(prevLogIndex <= 0) {
                replyBody = EchoBody(type = "error")
                System.err.println("Out of bounds previous log index ${prevLogIndex}")
            }
            val entry = entriesLog.get(prevLogIndex)
            if(entry != null || entry.term == body.prevLogTerm) {
                entriesLog.subList(prevLogIndex, entriesLog.size).clear()
                entriesLog.addAll(body.entries)
                commitIndex = minOf(entriesLog.size, body.leaderCommit)
                replyBody = EchoBody(type = "append_entries_res", term = term, success = true)
            }
        }
        val randMsgId = (0..10000).random()
        val msg= EchoMsg(randMsgId,body.candidateId?:"", replyBody, nodeId )
        val msgStr = mapper.writeValueAsString(msg)
        System.err.println("Append Entries Resp sent : ${msgStr}")
        System.out.println( msgStr)
        System.out.flush()
        lock.unlock()

    }

    fun matchIndex(){
        matchIndexMap.put(nodeId,entriesLog.size )
    }


    fun respondToVoteRequest(body:EchoBody){
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
            System.err.println("Our logs are both at term ${body.lastLogTerm} but our log is ${entriesLog.size} and their is body.lastLogIndex long, not granting vote")
        }
        else {
            System.err.println("Granting vote to ${body.candidateId}")
            grantVote = true
            votedFor = body.candidateId
            resetElectionDeadline()
        }
        lock.tryLock(5,TimeUnit.SECONDS)
        val replyBody = EchoBody("request_vote_res", term = term,voteGranted = grantVote )
        val randMsgId = (0..10000).random()
        val msg= EchoMsg(randMsgId,body.candidateId?:"", replyBody, nodeId )
        val msgStr = mapper.writeValueAsString(msg)
        System.err.println("Vote Request Resp sent : ${msgStr}")
        System.out.println( msgStr)
        System.out.flush()
        lock.unlock()




    }

    fun becomeFollower(){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        candidateState = "follower"
        resetElectionDeadline()
        matchIndexMap.clear()
        nextIndexMap.clear()
        System.err.println("Became follower for term :${term}")
        stateLock.unlock()

    }

    fun becomeLeader(){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        if(candidateState != "candidate"){
            System.err.println("Should be a candidate")
            return
        }
        candidateState = "leader"
        lastReplication = 0
        nextIndexMap.clear()
        matchIndexMap.clear()
        neighBorIds.map{
            nextIndexMap.put(it, entriesLog.size + 1)
            matchIndexMap.put(it,0)
        }
        resetStepDownDeadline()
        System.err.println("Became leader for term :${term}")
        stateLock.unlock()

    }

    fun replicateLog(){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        val elapsedTime = System.currentTimeMillis() - lastReplication
        var replicated = false
        if(candidateState == "leader" && minRepInterval < elapsedTime){
            neighBorIds.map{
                val ni =  nextIndexMap.get(it)?:0
                val prevLogTerm = entriesLog[ni-2].term
                val entriesToReplicate = entriesLog.slice(ni-1..entriesLog.size)
                if(entriesToReplicate.size > 0 || heartBeatInterval < elapsedTime){
                    System.err.println("Replicating ${ni} to ${it}")
                    replicated = true
                    val body = EchoBody("append_entries", term = term,leaderId = nodeId,prevLogIndex = ni-1, prevLogTerm = prevLogTerm )
                }
            }
        }
        stateLock.unlock()


    }



    fun sendVoteReq(){
        thread{
            neighBorIds.map{
                val randMsgId = (0..10000).random()
                val termForVoteRequested = term
                val replyBody = EchoBody(type = "request_vote", term = termForVoteRequested, candidateId = nodeId,lastLogIndex = entriesLog.size,lastLogTerm = entriesLog.last().term )
                val msg = EchoMsg(randMsgId, it, replyBody, nodeId)
                val msgStr = mapper.writeValueAsString(msg)
                // stateLock.tryLock(5,TimeUnit.SECONDS)
                System.err.println("Vote Req Msg Sent: ${msgStr}")
                // stateLock.unlock()
                val msgResp =   sendSyncMsg(msgStr)
                val body = msgResp.body
                val respTerm = body.term
                val voteGranted = body.voteGranted?:false
                maybeStepDown(respTerm?:0)
                if(candidateState == "candidate" && term == respTerm && term == termForVoteRequested && voteGranted ){
                    votes.add(msgResp.src)
                    System.err.println("Have votes :${votes}")
                    val majorityNo = getMajorityNumber()
                    if(majorityNo <= votes.size) becomeLeader()
                }


            }
        }
        resetStepDownDeadline()


    }

    fun sendSyncMsg(msg:String):EchoMsg{
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

    fun maybeStepDown(remoteTerm:Int){
        stateLock.tryLock(5,TimeUnit.SECONDS)
        if(term < remoteTerm){
            System.err.println("Stepping Down: remoteTerm: ${remoteTerm} Node term: ${term}")
            advanceTerm(remoteTerm)
            becomeFollower()
        }
        stateLock.unlock()


    }

    fun candidateScheduler(){
        Thread.sleep((0..100).random().toLong())
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                if(electionDeadline < System.currentTimeMillis())
                    if(candidateState != "leader" ){
                        becomeCandidate()
                    } else {
                        resetElectionDeadline()
                    }

            }
        }, 0, 100)

    }
    fun stepDownScheduler(){
        Timer().scheduleAtFixedRate( object : TimerTask() {
            override fun run() {
                if(electionDeadline < System.currentTimeMillis() && candidateState == "leader"){
                    System.err.println("Stepping down: haven't received any acks recently")
                    becomeFollower()
                }


            }
        }, 0, 100)

    }

    fun resetElectionDeadline(){
        lock.tryLock(5,TimeUnit.SECONDS)
        electionDeadline = System.currentTimeMillis() +  (electionTimeout*(Math.random()+1)).toLong()
        lock.unlock()
    }

    fun resetStepDownDeadline(){
        lock.tryLock(5,TimeUnit.SECONDS)
        stepDownDeadline = System.currentTimeMillis() +  (electionTimeout*(Math.random()+1)).toLong()
        lock.unlock()

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

}