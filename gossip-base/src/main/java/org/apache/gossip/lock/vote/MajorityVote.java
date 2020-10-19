/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gossip.lock.vote;

import org.apache.gossip.crdt.Crdt;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CRDT which used for distribute a votes for a given key.
 * 基于CRDT的分布式投票key
 */
public class MajorityVote implements Crdt<Map<String, VoteCandidate>, MajorityVote> {

  /**
   * <节点id，候选投注信息>
   */
  private final Map<String, VoteCandidate> voteCandidates = new ConcurrentHashMap<>();

  public MajorityVote(Map<String, VoteCandidate> voteCandidateMap) {
    voteCandidates.putAll(voteCandidateMap);
  }

  @Override
  public MajorityVote merge(MajorityVote other) {
    Map<String, VoteCandidate> mergedCandidates = new ConcurrentHashMap<>();
    Set<String> firstKeySet = this.voteCandidates.keySet();
    Set<String> secondKeySet = other.voteCandidates.keySet();
    //获取相同候选者交集
    Set<String> sameCandidatesSet = getIntersection(firstKeySet, secondKeySet);
    //获取并集
    Set<String> differentCandidatesSet = getIntersectionCompliment(firstKeySet, secondKeySet);
    // Merge different vote candidates by combining votes， 合并不同的投票
    for (String differentCandidateId : differentCandidatesSet) {
      if (this.voteCandidates.containsKey(differentCandidateId)) {
        mergedCandidates.put(differentCandidateId, this.voteCandidates.get(differentCandidateId));
      } else if (other.voteCandidates.containsKey(differentCandidateId)) {
        mergedCandidates.put(differentCandidateId, other.voteCandidates.get(differentCandidateId));
      }
    }
    // Merge votes for the same candidate， 合并相同的交集
    for (String sameCandidateId : sameCandidatesSet) {
      if (this.voteCandidates.containsKey(sameCandidateId) && other.voteCandidates
              .containsKey(sameCandidateId)) {
        mergedCandidates.put(sameCandidateId,
                mergeCandidate(this.voteCandidates.get(sameCandidateId),
                        other.voteCandidates.get(sameCandidateId)));
      }
    }

    return new MajorityVote(mergedCandidates);
  }

  /**
   * Merge different votes for same candidate
   * @param firstCandidate
   * @param secondCandidate
   * @return
   */
  private VoteCandidate mergeCandidate(VoteCandidate firstCandidate,
          VoteCandidate secondCandidate) {
    VoteCandidate mergeResult = new VoteCandidate(firstCandidate.getCandidateNodeId(),
            firstCandidate.getVotingKey(), new ConcurrentHashMap<>());
    Set<String> firstKeySet = firstCandidate.getVotes().keySet();
    Set<String> secondKeySet = secondCandidate.getVotes().keySet();
    //获取交集
    Set<String> sameVoteNodeSet = getIntersection(firstKeySet, secondKeySet);
    //获取并集
    Set<String> differentVoteNodeSet = getIntersectionCompliment(firstKeySet, secondKeySet);
    // Merge different voters by combining their votes，合并差集
    for (String differentCandidateId : differentVoteNodeSet) {
      if (firstCandidate.getVotes().containsKey(differentCandidateId)) {
        mergeResult.getVotes()
                .put(differentCandidateId, firstCandidate.getVotes().get(differentCandidateId));
      } else if (secondCandidate.getVotes().containsKey(differentCandidateId)) {
        mergeResult.getVotes()
                .put(differentCandidateId, secondCandidate.getVotes().get(differentCandidateId));
      }
    }
    // Merge vote for same voter， 合并相同的投注
    for (String sameVoteNodeId : sameVoteNodeSet) {
      if (firstCandidate.getVotes().containsKey(sameVoteNodeId) && secondCandidate.getVotes()
              .containsKey(sameVoteNodeId)) {
        mergeResult.getVotes().put(sameVoteNodeId,
                mergeVote(firstCandidate.getVotes().get(sameVoteNodeId),
                        secondCandidate.getVotes().get(sameVoteNodeId)));
      }
    }

    return mergeResult;
  }

  /**
   * Merge two votes from same voter
   * @param firstVote
   * @param secondVote
   * @return
   */
  private Vote mergeVote(Vote firstVote, Vote secondVote) {
    if (firstVote.getVoteValue().booleanValue() != secondVote.getVoteValue().booleanValue()) {
      if (firstVote.getVoteExchange()) {
        return firstVote;
      } else if (secondVote.getVoteExchange()) {
        return secondVote;
      } else {
        return secondVote;
      }
    } else {
      return secondVote;
    }
  }

  /**
   * 获取交集
   * @param first
   * @param second
   * @return
   */
  private Set<String> getIntersection(Set<String> first, Set<String> second) {
    Set<String> intersection = new HashSet<>(first);
    intersection.retainAll(second);
    return intersection;
  }

  /**
   * 获取差集
   * @param first
   * @param second
   * @return
   */
  private Set<String> getIntersectionCompliment(Set<String> first, Set<String> second) {
    Set<String> union = new HashSet<>();
    union.addAll(first);
    union.addAll(second);
    Set<String> intersectionCompliment = new HashSet<>(union);
    intersectionCompliment.removeAll(getIntersection(first, second));
    return intersectionCompliment;
  }

  @Override
  public Map<String, VoteCandidate> value() {
    Map<String, VoteCandidate> copy = new ConcurrentHashMap<>();
    copy.putAll(voteCandidates);
    return Collections.unmodifiableMap(copy);

  }

  @Override
  public int hashCode() {
    return voteCandidates.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (!(obj instanceof MajorityVote))
      return false;
    MajorityVote other = (MajorityVote) obj;
    return Objects.equals(voteCandidates, other.voteCandidates);
  }

  @Override
  public String toString() {
    return voteCandidates.toString();
  }

  @Override
  public MajorityVote optimize() {
    return new MajorityVote(voteCandidates);
  }

  public Map<String, VoteCandidate> getVoteCandidates() {
    return new ConcurrentHashMap<>(voteCandidates);
  }

}
