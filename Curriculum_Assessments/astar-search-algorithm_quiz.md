# A* Search Algorithm Assessment

## Part 1: True/False Questions (10)
1. **Question**: If the heuristic function h(n) in A* is admissible, it guarantees finding the optimal path even on a graph with negative edge weights (without negative cycles).
**Answer**: False.
**Mastery Explanation**: A* with an admissible heuristic guarantees optimality only for non-negative edge costs. If edge weights are negative, the standard A* (or Dijkstra's) may fail to find the optimal path because it assumes that extending a path will only increase its total cost, which leads to finalizing a node prematurely.

2. **Question**: A consistent heuristic is always admissible, but an admissible heuristic is not always consistent.
**Answer**: True.
**Mastery Explanation**: Consistency implies that h(n) <= c(n, n') + h(n') for all neighbors n'. By induction to the goal, this implies h(n) <= true_cost(n), which is the definition of admissibility. The reverse is not true.

3. **Question**: In A* search, if h(n) = 0 for all nodes, the algorithm reduces to Depth-First Search.
**Answer**: False.
**Mastery Explanation**: If h(n) = 0, A* uses only the exact cost g(n). This reduces A* to uniform-cost search (or Dijkstra's algorithm), not Depth-First Search.

4. **Question**: When using a non-admissible heuristic, A* search may find a suboptimal path, but it is guaranteed to expand fewer nodes than with an admissible heuristic.
**Answer**: False.
**Mastery Explanation**: A non-admissible heuristic might cause the search to be misled and explore vast areas of the search space unnecessarily, thus it is not guaranteed to expand fewer nodes.

5. **Question**: If a graph is a tree, A* search using any heuristic will never re-open a node once it is placed in the closed set.
**Answer**: True.
**Mastery Explanation**: In a tree, there is only one path to any node. Thus, the first time a node is reached, it is via the optimal (and only) path. It will never be reached again, let alone with a cheaper cost.

6. **Question**: Memory consumption in standard A* search is bounded by O(b*d), where b is the branching factor and d is the depth.
**Answer**: False.
**Mastery Explanation**: Standard A* keeps all generated nodes in memory (in open and closed sets), giving it a space complexity of O(b^d), which is exponential, not linear. Algorithms like IDA* are used to reduce memory consumption to O(b*d).

7. **Question**: If h1(n) and h2(n) are both admissible heuristics, then h3(n) = max(h1(n), h2(n)) is also admissible and will dominate or equal both.
**Answer**: True.
**Mastery Explanation**: Since both h1 and h2 never overestimate the true cost, their maximum also will never overestimate the true cost. Taking the maximum creates a more informed heuristic.

8. **Question**: A* requires a priority queue (min-heap) to operate efficiently; using an unsorted array would degrade its time complexity drastically.
**Answer**: True.
**Mastery Explanation**: A* needs to repeatedly find the node with the minimum f-value. A min-heap does this in O(log N), while an unsorted array takes O(N), changing the overall time complexity from O(E log V) to O(V^2).

9. **Question**: Bidirectional A* search is guaranteed to meet in the middle and terminate exactly when the two search frontiers intersect.
**Answer**: False.
**Mastery Explanation**: When the forward and backward frontiers of a bidirectional A* intersect, the path found is not necessarily optimal. The search must continue until the minimum f-value of the open sets is greater than or equal to the cost of the best path found so far.

10. **Question**: The time complexity of A* is always better than Dijkstra's algorithm.
**Answer**: False.
**Mastery Explanation**: In the worst-case scenario (e.g., an uninformative heuristic or specific graph topologies), A* will expand the same number of nodes as Dijkstra's algorithm, yielding the same time complexity. The heuristic improves the average case, not necessarily the worst case.

## Part 2: Multiple Choice Questions (15)
11. **Question**: Which of the following conditions is sufficient to guarantee that A* finds an optimal path on a finite graph with non-negative edge costs?
A) The heuristic is consistent.
B) The heuristic is admissible.
C) The heuristic never underestimates the true cost.
D) The heuristic is non-monotonic.
**Answer**: B.
**Mastery Explanation**: An admissible heuristic (which never overestimates the true cost) is the necessary and sufficient condition for optimality in tree search, and ensures optimality in graph search if reopened properly. Consistency implies admissibility.

12. **Question**: In an implementation of A* search on a grid where agents can move diagonally as well as orthogonally, which heuristic is typically the most accurate and admissible?
A) Manhattan distance
B) Euclidean distance
C) Octile distance
D) Chebyshev distance
**Answer**: C.
**Mastery Explanation**: Octile distance accurately accounts for the differing costs of orthogonal (cost 1) and diagonal (cost sqrt(2)) moves on a grid. Manhattan distance overestimates if diagonal moves are allowed. Chebyshev assumes orthogonal and diagonal moves cost the same. Euclidean is admissible but less informed than Octile on a grid.

13. **Question**: What is the primary purpose of the CLOSED set in A* search?
A) To maintain the path from start to goal.
B) To prevent infinite loops and avoid re-evaluating nodes.
C) To store nodes that have not yet been evaluated.
D) To estimate the remaining cost to the goal.
**Answer**: B.
**Mastery Explanation**: The CLOSED set keeps track of nodes that have already been evaluated. This prevents redundant evaluations of the same state and infinite loops in cyclic graphs.

14. **Question**: If A* is run with an overestimating heuristic (h(n) > true cost), what is the most likely outcome?
A) The algorithm will crash.
B) The algorithm will run indefinitely.
C) The algorithm will find a suboptimal path faster.
D) The algorithm will find the optimal path but slower.
**Answer**: C.
**Mastery Explanation**: An overestimating heuristic makes A* act more like a greedy best-first search. It focuses heavily on paths that appear to go straight to the goal, often finding a solution quickly, but it loses the guarantee of optimality.

15. **Question**: Which memory-bounded variation of A* performs a series of depth-first searches with increasing cost bounds?
A) SMA*
B) Weighted A*
C) IDA*
D) D* Lite
**Answer**: C.
**Mastery Explanation**: Iterative Deepening A* (IDA*) uses an iterative deepening depth-first search strategy where the depth bound is defined by the f-cost, using O(b*d) memory.

16. **Question**: In A*, the evaluation function is f(n) = g(n) + h(n). What happens if g(n) is weighted less than h(n), e.g., f(n) = g(n) + w * h(n) where w > 1?
A) It behaves exactly like Dijkstra's algorithm.
B) It becomes Breadth-First Search.
C) It trades optimality for speed (Weighted A*).
D) It guarantees finding the optimal path with fewer node expansions.
**Answer**: C.
**Mastery Explanation**: This is Weighted A*. By inflating the heuristic (w > 1), the search becomes greedier, expanding nodes closer to the goal faster, but it typically sacrifices the guarantee of optimality in exchange for computational speed.

17. **Question**: During A* graph search, if a shorter path to a node already in the OPEN set is found, what should the algorithm do?
A) Ignore it.
B) Update the node's g-value and parent, and adjust its position in the priority queue.
C) Move the node to the CLOSED set.
D) Add a duplicate node to the OPEN set.
**Answer**: B.
**Mastery Explanation**: If a cheaper path to a frontier node is discovered, its current g-value is updated to reflect this cheaper cost, its parent pointer is updated, and its priority (f-value) is decreased, requiring a decrease-key operation in the priority queue.

18. **Question**: Which of the following is true regarding Tie-breaking in A*?
A) Tie-breaking does not affect the number of nodes expanded.
B) Breaking ties in favor of larger g-values generally expands fewer nodes.
C) Breaking ties in favor of smaller g-values is computationally optimal.
D) Tie-breaking randomly guarantees the best average-case performance.
**Answer**: B.
**Mastery Explanation**: Breaking ties in favor of higher g-values (and therefore lower h-values for a given f) means preferring nodes closer to the goal. This reduces the number of nodes expanded along the optimal path frontier.

19. **Question**: A* is optimally efficient for a given heuristic. What does this mean?
A) It takes the minimum possible time to run.
B) No other optimal algorithm that expands search paths from the root using the same heuristic will expand fewer nodes.
C) It uses the least amount of memory possible.
D) It can find the goal in O(1) time.
**Answer**: B.
**Mastery Explanation**: "Optimally efficient" means that any other algorithm that guarantees finding the optimal solution using the same admissible heuristic will expand at least as many nodes as A* (ignoring tie-breaking).

20. **Question**: When dealing with dynamic environments where edge costs increase over time, which algorithm is an extension of A* designed to handle this efficiently without full replanning?
A) IDA*
B) D* / D* Lite
C) SMA*
D) Jump Point Search
**Answer**: B.
**Mastery Explanation**: D* (Dynamic A*) and D* Lite are designed for incremental replanning. They efficiently update the existing search tree when edge costs change, rather than searching from scratch like standard A*.

21. **Question**: What is the impact of a heuristic that is computationally expensive to evaluate?
A) It reduces the number of nodes expanded, leading to overall faster search times.
B) It might reduce the number of nodes expanded, but the total time could increase if the heuristic calculation dominates.
C) It has no effect on total time, only on memory usage.
D) It will always make the search faster than using a simple heuristic.
**Answer**: B.
**Mastery Explanation**: There is a trade-off. A complex heuristic might provide tighter bounds and expand fewer nodes, but if calculating h(n) takes too long, the overall execution time (Nodes_Expanded * Time_Per_Node) may be worse than using a simpler, faster-to-compute heuristic.

22. **Question**: Which technique can dramatically speed up A* on uniform cost grids by skipping over large symmetric areas?
A) Bidirectional Search
B) Hierarchical Pathfinding
C) Jump Point Search (JPS)
D) Iterative Deepening
**Answer**: C.
**Mastery Explanation**: Jump Point Search is a specialized optimization for A* on uniform-cost grids. It prunes the search space by selectively "jumping" over nodes that do not represent turning points (jump points), significantly speeding up the search.

23. **Question**: In what scenario does A* degenerate exactly to Dijkstra's Algorithm?
A) When h(n) is the exact true cost to the goal.
B) When h(n) overestimates the true cost.
C) When the graph is a DAG.
D) When h(n) = 0 for all nodes.
**Answer**: D.
**Mastery Explanation**: If the heuristic provides no information (h(n) = 0), the evaluation function is simply f(n) = g(n). This is the exact mechanism of Dijkstra's algorithm (Uniform Cost Search).

24. **Question**: When using A* for finding a path in a state space with highly varying action costs (e.g., 1 vs 1000), what problem might arise with a weak heuristic?
A) The algorithm will fail to find a path.
B) The search space explored might look similar to breadth-first search, heavily expanding low-cost areas first.
C) The algorithm will terminate prematurely.
D) The algorithm requires a max-heap instead of a min-heap.
**Answer**: B.
**Mastery Explanation**: With highly variable costs, a weak heuristic will not effectively guide the search past low-cost local minima. A* will behave like Dijkstra's, exhausting all cheap actions before taking a necessary but expensive action.

25. **Question**: What is the main disadvantage of SMA* (Simplified Memory-Bounded A*)?
A) It is not optimal.
B) It cannot be used with admissible heuristics.
C) It may "thrash", repeatedly dropping and re-generating the same nodes if the memory limit is too small.
D) It only works on tree structures.
**Answer**: C.
**Mastery Explanation**: SMA* uses all available memory. When memory is full, it drops the least promising nodes to make room. If the memory is too constrained, it may constantly drop nodes it later needs, leading to severe performance degradation known as thrashing.

## Part 3: "Small Twist" Questions (15)
26. **Question**: Twist: You implement A* on a grid, but allow agents to travel through corner-adjacent obstacles (e.g., diagonal movement when flanking walls). How does this affect the admissibility of the Euclidean distance heuristic?
**Answer**: It does not affect it; Euclidean distance remains admissible.
**Mastery Explanation**: Euclidean distance is the shortest possible straight-line distance in continuous space. Since grid paths (even allowing "squeezing" through corners) can never be shorter than a straight line, Euclidean distance remains a lower bound.

27. **Question**: Twist: You use a consistent heuristic, but your graph has a few negative edge weights (no negative cycles). Does A* guarantee optimality?
**Answer**: No.
**Mastery Explanation**: A* fundamentally relies on the assumption that path costs are monotonically non-decreasing. Negative edges violate this. A node might be placed in the CLOSED set with an assumed optimal cost, but a longer path involving negative edges might actually be cheaper, which standard A* will miss.

28. **Question**: Twist: You are using A*, but your g(n) function is slightly modified: g(n) is the cost from start to n, PLUS a small penalty 'k' for each step taken. How does this affect the search?
**Answer**: The algorithm now optimizes for a balance between path cost and minimizing the number of steps (path length).
**Mastery Explanation**: By adding a per-step penalty, you are essentially increasing all edge weights uniformly by 'k'. This penalizes paths with many edges. A* will still find optimal paths under this new cost metric, but the resulting path will favor fewer steps.

29. **Question**: Twist: You scale down the heuristic by half (h(n) = 0.5 * true_h(n)). Is the search still optimal?
**Answer**: Yes.
**Mastery Explanation**: Scaling down an admissible heuristic keeps it admissible (it still won't overestimate). The search remains optimal, but it will be less informed and will expand more nodes than using the full heuristic.

30. **Question**: Twist: You implement A* but accidentally use a LIFO (stack) structure instead of a priority queue for nodes with the exact same f-value. What happens?
**Answer**: The algorithm remains optimal, but tie-breaking behaves like depth-first search for nodes with the same f-value.
**Mastery Explanation**: Using LIFO for tie-breaking means the algorithm will plunge deep into a promising branch before expanding laterally. This often improves performance if the heuristic is good, as it pushes towards the goal.

31. **Question**: Twist: In an A* implementation, you decide not to update the g-value of nodes already in the OPEN set if a better path is found, but just add a duplicate node. Does this break the algorithm?
**Answer**: No, it does not break optimality, but it wastes memory and time.
**Mastery Explanation**: Adding duplicates (often called "lazy A*") works because the optimal path (lowest g-value) will be popped from the queue first. The suboptimal duplicates will be ignored later if you check the CLOSED set properly upon popping. It's valid but inefficient.

32. **Question**: Twist: The goal state in your A* problem is moving. How must you adapt standard A*?
**Answer**: You must continuously replan or use algorithms designed for moving targets (e.g., Moving Target Search).
**Mastery Explanation**: Standard A* calculates heuristics based on a fixed goal. If the goal moves, the heuristic values (and potentially the f-values in the OPEN set) become invalid. You either have to recalculate h(n) frequently or use specialized algorithms.

33. **Question**: Twist: You apply A* to a problem where the state space is a continuous plane. Can standard A* be used directly?
**Answer**: No.
**Mastery Explanation**: A* requires a discrete set of states and actions to branch from. Continuous spaces have infinite branching factors. The space must be discretized (e.g., using a grid, navmesh, or probabilistic roadmaps) before A* can be applied.

34. **Question**: Twist: You use the Manhattan distance heuristic, but your grid allows teleportation between specific distant nodes for a cost of 1. Is the heuristic still admissible?
**Answer**: No.
**Mastery Explanation**: Teleportation might allow reaching the goal with a true cost much lower than the Manhattan distance. The heuristic will overestimate the true cost for nodes near a teleporter, breaking admissibility and potentially optimality.

35. **Question**: Twist: Your graph search implementation of A* does not have a CLOSED set. What is the risk on a finite grid without obstacles?
**Answer**: Performance degradation due to redundant path exploration, but no infinite loops (if costs are > 0).
**Mastery Explanation**: On a finite grid with positive costs, f-values will strictly increase. Without a CLOSED set, A* will explore multiple paths to the same node, acting more like tree search. It will eventually finish, but will expand exponentially more nodes.

36. **Question**: Twist: You have multiple goal nodes, and any one is acceptable. How do you modify h(n)?
**Answer**: h(n) should be the minimum of the admissible heuristic estimates to all individual goals: h(n) = min(h(n, g_i)).
**Mastery Explanation**: To maintain admissibility, the heuristic must not overestimate the cost to the *closest* goal. Taking the minimum of estimates to all valid goals ensures this.

37. **Question**: Twist: You reverse A* to search from the Goal to the Start. Does the heuristic need to change?
**Answer**: Yes, the heuristic must estimate the cost from the current node to the *Start* node, not the Goal node.
**Mastery Explanation**: When searching backwards, the roles of start and goal are swapped. The heuristic h(n) must reflect the estimated cost from node n to the actual target of the current search, which is the original start node.

38. **Question**: Twist: Every edge in your graph has a cost of exactly 0. What does A* do?
**Answer**: It relies entirely on the heuristic to guide the search, acting exactly like Greedy Best-First Search.
**Mastery Explanation**: If all g-costs are 0, f(n) = 0 + h(n) = h(n). The priority queue will sort solely based on the heuristic.

39. **Question**: Twist: You are using an admissible heuristic, but due to floating-point precision errors, h(n) sometimes evaluates to slightly more than the true cost (e.g., by 0.00001). What happens?
**Answer**: Strict optimality is theoretically lost.
**Mastery Explanation**: Admissibility requires h(n) <= true_cost *always*. Even microscopic overestimations can cause A* to prune the true optimal path if a suboptimal path has a lower computed f-value due to the error.

40. **Question**: Twist: You run A* on a Rubik's Cube. What is a common, highly effective admissible heuristic?
**Answer**: Pattern databases.
**Mastery Explanation**: Pattern databases precompute the exact minimum number of moves to solve sub-problems (e.g., just the corners). Because solving a sub-problem is a relaxation of the full problem, looking up this precomputed cost provides a perfectly admissible and highly informed heuristic.

## Part 4: Coding & Debugging Questions (10)
41. **Question**: In this Python snippet, what critical A* logic bug will cause it to return suboptimal paths?
```python
def a_star(start, goal):
    open_set = [(0, start)]
    closed_set = set()
    g_score = {start: 0}
    while open_set:
        current = heappop(open_set)[1]
        if current == goal: return reconstruct_path(current)
        closed_set.add(current)
        for neighbor in get_neighbors(current):
            if neighbor in closed_set: continue
            tentative_g = g_score[current] + cost(current, neighbor)
            if neighbor not in g_score:
                g_score[neighbor] = tentative_g
                f_score = tentative_g + heuristic(neighbor, goal)
                heappush(open_set, (f_score, neighbor))
```
**Answer**: The code fails to update nodes that are already in `g_score` (and the open set) if a cheaper `tentative_g` is found.
**Mastery Explanation**: The condition `if neighbor not in g_score:` only handles newly discovered nodes. If a node was reached previously via a more expensive path, A* must update its `g_score` and push the better `f_score` to the queue.

42. **Question**: You notice your A* implementation consumes massive amounts of RAM and crashes with Out Of Memory on large maps. Which data structure is the likely culprit, and how can you optimize it?
**Answer**: The `closed_set` or the parent pointer dictionary. Optimization: Use bit-packing for visited states or migrate to a memory-bounded variant like IDA*.
**Mastery Explanation**: In large state spaces, storing every visited node in memory (typically a Hash Set/Dict) leads to O(b^d) memory exhaustion.

43. **Question**: A developer implements a priority queue for A* using a standard list and calls `.sort()` after every insertion. What is the impact on time complexity?
**Answer**: Insertion becomes O(N log N) instead of O(log N).
**Mastery Explanation**: Sorting the entire list upon every neighbor generation turns a fast queue operation into a severe bottleneck, completely destroying A*'s performance advantage on large graphs. A proper Min-Heap must be used.

44. **Question**: Debug this heuristic function for a grid with obstacles:
```python
def heuristic(node, goal):
    return abs(node.x - goal.x) + abs(node.y - goal.y) + obstacle_penalty(node)
```
**Answer**: The heuristic is likely inadmissible.
**Mastery Explanation**: Adding an `obstacle_penalty` based on local geometry can cause the heuristic to overestimate the true cost to the goal, breaking admissibility and causing A* to return suboptimal paths.

45. **Question**: Your C++ A* code uses `std::priority_queue`. To update the f-value of an existing node in the OPEN set, the developer searches the queue, removes the element, and re-inserts it. Why is this bad, and what's the standard workaround?
**Answer**: `std::priority_queue` does not support O(log N) updates or O(1) searches. The standard workaround is "lazy deletion" (pushing duplicates and ignoring stale ones upon pop).
**Mastery Explanation**: Modifying standard library priority queues is O(N). Pushing a new duplicate entry with the updated, lower f-value is O(log N). When a node is popped, you check if its g-value matches the best known g-value; if not, it's a stale duplicate and you `continue`.

46. **Question**: You are profiling your A* code and find that the `hash()` function on state objects is taking 60% of CPU time. Why?
**Answer**: State objects are complex (e.g., entire game boards), and the hash function is evaluating the entire deep structure for the `closed_set` lookups.
**Mastery Explanation**: A* performs millions of Set lookups. If the state representation is large, hashing it is slow. States compression into integer keys (e.g., Zobrist hashing) is needed.

47. **Question**: Consider the reconstruction phase of A*:
```python
def reconstruct_path(came_from, current):
    path = [current]
    while current in came_from:
        current = came_from[current]
        path.append(current)
    return path
```
What is wrong with the output of this function?
**Answer**: The path is returned backwards (from goal to start).
**Mastery Explanation**: Tracing parent pointers from the goal leads back to the start. The resulting list must be reversed before returning it to the user.

48. **Question**: In a multithreaded environment, you attempt to speed up A* by running multiple agents exploring the OPEN set concurrently. What is the primary synchronization challenge?
**Answer**: Safely updating the shared OPEN (priority queue) and CLOSED sets without excessive lock contention.
**Mastery Explanation**: A* is inherently sequential. Concurrent access requires locking the min-heap. Lock contention will likely negate any multithreading benefits.

49. **Question**: A bug report states: "A* gets stuck in an infinite loop on Map 4." Upon inspection, Map 4 has teleporters that connect back to previous areas, with a traversal cost of 0. Why is A* looping?
**Answer**: 0-cost cycles.
**Mastery Explanation**: If a cycle has a net cost of 0, A* can continually traverse it without increasing the g-score, repeatedly evaluating the same nodes if the CLOSED set logic doesn't strictly prevent re-expansion of equal-cost paths.

50. **Question**: Your A* agent moves diagonally on a grid. The cost of orthogonal movement is 1, and diagonal is 1.414. You use Manhattan distance for the heuristic. Is the resulting path guaranteed optimal?
**Answer**: No, Manhattan is inadmissible here.
**Mastery Explanation**: Manhattan distance (dx + dy) evaluates the diagonal move to cost 2 (1 up, 1 right). The actual cost is 1.414. Because h(n) > true cost, the heuristic overestimates, losing admissibility and optimality.
