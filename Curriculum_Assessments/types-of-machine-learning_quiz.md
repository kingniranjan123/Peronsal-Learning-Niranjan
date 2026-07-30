# Types of Machine Learning - Elite Assessment

## Part 1: True/False Questions (10)

1. **Question:** In semi-supervised learning, pseudo-labeling leverages high-confidence predictions on unlabeled data to iteratively retrain the model, fundamentally relying on the cluster assumption or the manifold assumption to avoid compounding errors.
**Answer:** True
**Mastery Explanation:** Pseudo-labeling works by assuming that decision boundaries lie in low-density regions (cluster assumption) or that high-dimensional data lie on a lower-dimensional manifold. Without these, pseudo-labels could reinforce incorrect biases.

2. **Question:** In Reinforcement Learning, Off-policy algorithms like Q-learning evaluate and improve the same policy that is used to select actions, making them highly stable with non-linear function approximators.
**Answer:** False
**Mastery Explanation:** Q-learning is an *off-policy* algorithm, meaning it learns the value of the optimal policy independently of the agent's actions. However, off-policy learning combined with non-linear function approximators (like neural networks) and bootstrapping leads to the "deadly triad" of instability.

3. **Question:** Self-supervised learning is strictly a subset of unsupervised learning where the learning objective is derived by predicting hidden parts of the input from the unhidden parts (e.g., masked language modeling).
**Answer:** True
**Mastery Explanation:** SSL creates artificial supervisory signals from the data itself. The labels are automatically generated from the input (e.g., predicting the next word, predicting masked patches).

4. **Question:** Contrastive Learning, a common self-supervised technique, relies exclusively on minimizing the distance between positive pairs without needing negative samples to prevent representation collapse.
**Answer:** False
**Mastery Explanation:** Standard Contrastive Learning requires negative samples (e.g., InfoNCE loss) to push dissimilar representations apart, preventing representation collapse (where the model outputs a constant vector for all inputs). Only specialized methods like BYOL or SimSiam avoid negative samples using asymmetrical architectures.

5. **Question:** Transductive learning algorithms build a general model over the entire input space and can seamlessly predict labels for completely unseen data points not present during training.
**Answer:** False
**Mastery Explanation:** Transductive learning specifically infers labels for a given set of unlabeled data points provided during the training phase. It does not generalize to completely unseen data (which is inductive learning).

6. **Question:** Active learning reduces the labeling bottleneck by querying an oracle for labels of data points that maximize model uncertainty or expected model change.
**Answer:** True
**Mastery Explanation:** The core premise of active learning is strategically sampling the most informative points (e.g., using entropy, margin, or least confidence) for manual labeling.

7. **Question:** One-shot learning inherently requires zero gradient updates at inference time because the model learns to generalize from a single example via meta-learning algorithms like MAML.
**Answer:** False
**Mastery Explanation:** Algorithms like MAML (Model-Agnostic Meta-Learning) explicitly perform a small number of gradient updates (fine-tuning) on the support set at inference/test time.

8. **Question:** In supervised learning, structural risk minimization involves minimizing both the empirical risk on the training data and a regularization term bound by the VC dimension of the hypothesis space.
**Answer:** True
**Mastery Explanation:** Vapnik-Chervonenkis (VC) theory states that generalization error is bounded by empirical risk plus a complexity term depending on the VC dimension (capacity) of the model class.

9. **Question:** Transfer learning in deep neural networks typically involves freezing the final layers and fine-tuning the initial convolutional/linear layers because earlier layers capture highly task-specific semantic features.
**Answer:** False
**Mastery Explanation:** It is the opposite. Earlier layers capture generic, low-level features (edges, textures), while later layers capture task-specific, high-level semantic features. Transfer learning usually freezes earlier layers and trains the final layers.

10. **Question:** Multi-instance learning is a form of weakly supervised learning where labels are assigned to bags of instances rather than individual instances, and a bag is positive if at least one instance inside is positive.
**Answer:** True
**Mastery Explanation:** The Standard Multi-Instance Learning (MIL) assumption is exactly this: a bag is positive if it contains at least one positive instance, and negative if all instances are negative.

## Part 2: Multiple Choice Questions (15)

11. **Question:** Which of the following best describes the difference between Inductive and Transductive Learning?
A) Inductive learning builds a general function mapping inputs to outputs; Transductive learning predicts labels for specific unlabelled examples provided at training time.
B) Inductive learning requires labeled data; Transductive learning requires only unlabeled data.
C) Inductive learning applies to classification tasks; Transductive learning applies to regression tasks.
D) There is no difference; they are synonymous.
**Answer:** A
**Mastery Explanation:** Transductive learning optimizes predictions for a specific set of test points known at training, avoiding the harder problem of learning a globally generalizing function (Inductive).

12. **Question:** In the context of Reinforcement Learning, what characterizes an Actor-Critic architecture?
A) It uses a single network to output both policy and value predictions without shared representations.
B) The 'Actor' updates the policy distribution, while the 'Critic' estimates the value function to reduce the variance of policy gradient updates.
C) The 'Actor' explores the environment randomly, while the 'Critic' acts greedily.
D) It relies entirely on a model of the environment dynamics (Model-based RL).
**Answer:** B
**Mastery Explanation:** The Actor computes the policy (actions), and the Critic evaluates the action by computing the value function. The Critic's baseline reduces the high variance inherent in standard REINFORCE policy gradients.

13. **Question:** When applying Self-Taught Learning (a form of transfer learning), what is the primary requirement for the unlabeled data used in the first phase?
A) It must be drawn from the exact same marginal distribution as the labeled data.
B) It must share the same label space as the target task.
C) It does not need to have the same class labels or even be drawn from the exact same distribution, as long as it helps learn underlying basic features.
D) It must consist of paired positive and negative samples.
**Answer:** C
**Mastery Explanation:** Self-taught learning (e.g., learning basic edge detectors from random images) does not require the unlabeled data to be from the same distribution or classes, unlike standard semi-supervised learning.

14. **Question:** In Generative Adversarial Networks (GANs), what is the theoretical optimum for the discriminator when the generator perfectly models the data distribution?
A) The discriminator outputs 1.0 for all inputs.
B) The discriminator outputs 0.0 for all inputs.
C) The discriminator outputs 0.5 for all inputs.
D) The discriminator oscillates between 0.0 and 1.0.
**Answer:** C
**Mastery Explanation:** When the generated distribution perfectly matches the true data distribution (Pg = Pdata), the discriminator cannot distinguish between real and fake samples, resulting in a probability of 0.5.

15. **Question:** What is the primary advantage of Meta-Learning (Learning to Learn) over standard Transfer Learning?
A) It strictly requires less training data across all tasks combined.
B) It optimizes the model's initialization or learning algorithm such that it can rapidly adapt to new, unseen tasks with minimal data.
C) It eliminates the need for gradient descent entirely.
D) It only uses unsupervised data.
**Answer:** B
**Mastery Explanation:** Meta-learning trains a model across a distribution of tasks to learn how to adapt quickly (e.g., finding an optimal set of initial weights in MAML), whereas transfer learning often just leverages pre-trained features from a single massive task.

16. **Question:** Which assumption is NOT fundamentally necessary for Semi-Supervised Learning to be effective?
A) Smoothness Assumption
B) Cluster Assumption
C) Manifold Assumption
D) Independence Assumption (Features are conditionally independent given the class)
**Answer:** D
**Mastery Explanation:** The Independence assumption is specific to Naive Bayes. Semi-supervised learning relies on smoothness (close points share labels), cluster (decision boundaries in low density), or manifold (data lies on a lower-dimensional manifold) assumptions.

17. **Question:** Weakly Supervised Learning encompasses several paradigms. Which of the following is an example of 'Inexact Supervision'?
A) Training a model using labels generated by heuristics or rules (e.g., Snorkel).
B) Training a model using image-level labels to perform object detection (bounding boxes).
C) Training a model where some labels are intentionally flipped to simulate noise.
D) Training a model with only 1% of the dataset labeled, and the rest unlabeled.
**Answer:** B
**Mastery Explanation:** Inexact supervision occurs when the provided labels are coarser than the desired output (e.g., having 'dog' label for an image, but wanting to find the bounding box of the dog).

18. **Question:** In Multi-Task Learning (MTL), what is the phenomenon of 'Negative Transfer'?
A) When sharing representations between tasks causes a decrease in performance on one or more tasks compared to learning them independently.
B) When gradients from different tasks perfectly align, causing the model to overfit.
C) When the model fails to learn anything because all tasks have contradictory labels.
D) When transferring weights from a pre-trained model to a new model causes catastrophic forgetting.
**Answer:** A
**Mastery Explanation:** Negative transfer happens in MTL when forcing tasks to share parameters hurts performance, usually because the tasks are weakly related or actively competing for network capacity.

19. **Question:** Which statement best describes the difference between Model-free and Model-based Reinforcement Learning?
A) Model-free RL requires a neural network; Model-based RL uses decision trees.
B) Model-based RL explicitly learns the transition dynamics and reward functions of the environment to plan; Model-free RL directly learns a policy or value function.
C) Model-free RL operates in continuous action spaces; Model-based RL only works in discrete spaces.
D) Model-based RL cannot be used in episodic environments.
**Answer:** B
**Mastery Explanation:** Model-based algorithms build a predictive model of the environment (P(s', r | s, a)) and use it for planning (e.g., MCTS, Dyna). Model-free algorithms (like Q-learning or PPO) learn directly from experience without building this internal model.

20. **Question:** What is the primary purpose of 'Label Smoothing' in supervised classification?
A) To convert discrete labels into continuous variables for regression.
B) To prevent the model from becoming overly confident in its predictions, thereby acting as a regularizer.
C) To interpolate between noisy labels and true labels.
D) To balance class frequencies in the loss function.
**Answer:** B
**Mastery Explanation:** Label smoothing changes one-hot targets to a mix of the true class and a uniform distribution, penalizing overconfidence and improving generalization and model calibration.

21. **Question:** In Federated Learning, what is specifically communicated between the client devices and the central server?
A) The raw data batches.
B) The model parameters or gradients.
C) The pseudo-labels of the data.
D) The hyperparameter configurations only.
**Answer:** B
**Mastery Explanation:** Federated Learning allows decentralized devices to collaboratively train a model by sharing local model updates (weights or gradients) with a central server, ensuring raw data never leaves the local device.

22. **Question:** Which of the following is a classic algorithm used for Unsupervised Anomaly Detection?
A) Support Vector Classification (SVC)
B) Isolation Forest
C) Random Forest
D) Linear Discriminant Analysis (LDA)
**Answer:** B
**Mastery Explanation:** Isolation Forest is an unsupervised algorithm that detects anomalies by isolating instances using random splits; anomalies are easier to isolate and thus have shorter path lengths in the trees.

23. **Question:** In Few-Shot Learning, a 'N-way K-shot' classification task means:
A) There are N classes, and the model has K iterations to learn them.
B) There are N support sets, and each contains K classes.
C) There are N classes, and the model is provided K labeled examples per class.
D) The model uses a neural network with N layers and K attention heads.
**Answer:** C
**Mastery Explanation:** Standard terminology in few-shot learning defines N-way as the number of classes in the task, and K-shot as the number of labeled examples provided for each of those classes.

24. **Question:** Continuous Learning (or Lifelong Learning) models often suffer from Catastrophic Forgetting. Which technique is commonly used to mitigate this?
A) Dropping out 50% of the neurons randomly.
B) Elastic Weight Consolidation (EWC).
C) Increasing the learning rate for new tasks.
D) Replacing ReLU activations with Sigmoid.
**Answer:** B
**Mastery Explanation:** EWC slows down learning on weights that are important to previously learned tasks (measured by the Fisher Information Matrix), preserving old knowledge while learning new tasks.

25. **Question:** Zero-Shot Learning (ZSL) relies heavily on what kind of auxiliary information to classify unseen classes?
A) Extensive hyperparameter tuning.
B) A large number of unlabeled samples from the unseen classes.
C) Semantic representations, such as attributes or word embeddings, linking seen and unseen classes.
D) Meta-learning over hundreds of similar tasks.
**Answer:** C
**Mastery Explanation:** ZSL maps input data to a semantic space (like word vectors or visual attributes). When an unseen class appears, the model projects it into this space and finds the closest semantic description.

## Part 3: Small Twist Questions (15)

26. **Question:** You are training a supervised classification model on an imbalanced dataset. You use Class Weights in the Cross-Entropy loss to penalize errors on the minority class. 
*Twist:* You switch your evaluation metric from Accuracy to F1-score, and notice the F1-score drops drastically compared to using no class weights. Why?
**Answer:** The class weights caused the model to aggressively predict the minority class, significantly increasing False Positives. While this increases Recall, it destroys Precision. Since F1 is the harmonic mean of Precision and Recall, a catastrophic drop in Precision tanks the F1-score.
**Mastery Explanation:** Weighting the loss heavily biases the decision boundary towards the minority class. This reduces False Negatives but increases False Positives.

27. **Question:** You apply PCA (Unsupervised) to reduce dimensionality before training a Logistic Regression (Supervised) model.
*Twist:* You accidentally fit the PCA on the combined Train + Test dataset, rather than just the Train set. What happens?
**Answer:** Data Leakage occurs.
**Mastery Explanation:** Because PCA uses the variance of the entire dataset to compute principal components, information about the distribution of the test set leaks into the training pipeline, artificially inflating test performance and violating the principle of blind evaluation.

28. **Question:** You are implementing a Q-learning agent. It converges nicely.
*Twist:* You change the environment such that rewards are stochastic rather than deterministic. The agent suddenly overestimates action values and performs poorly. Why?
**Answer:** Maximization Bias.
**Mastery Explanation:** Q-learning uses `max_a Q(s', a)` to bootstrap. With stochastic rewards, random positive noise causes the `max` operator to systematically overestimate the true Q-values. Double Q-learning is needed to separate action selection from action evaluation.

29. **Question:** You use a pre-trained ResNet50 for Transfer Learning by freezing all layers and training a new linear classifier on top. It works decently.
*Twist:* Your target dataset consists of medical X-rays, while ResNet was trained on ImageNet (natural images). The performance is abysmal. Why?
**Answer:** The feature distributions are entirely disparate (Negative Transfer).
**Mastery Explanation:** Freezing layers assumes the pre-trained features are generic enough for the target task. X-rays lack the color, texture, and shapes of ImageNet. You must fine-tune the entire network or train from scratch because the target domain is too far from the source domain.

30. **Question:** In an Active Learning setup, you use Uncertainty Sampling (selecting points where the model is least confident).
*Twist:* Your dataset contains a high percentage of mislabeled/noisy outliers. The active learning performance plateaus lower than random sampling. Why?
**Answer:** The model gets stuck querying inherently unlearnable noisy points.
**Mastery Explanation:** Outliers or corrupted data will consistently have high uncertainty. Uncertainty sampling will waste all the labeling budget on these useless points instead of informative points near the true decision boundary.

31. **Question:** You are using Semi-Supervised Learning with a consistency regularization loss (e.g., penalizing the model if predictions change when data is augmented).
*Twist:* You apply highly aggressive augmentations (e.g., cropping out 90% of the image) to the unlabeled data. The model collapses. Why?
**Answer:** Violation of the Manifold/Smoothness assumption due to semantic destruction.
**Mastery Explanation:** Consistency regularization assumes that augmenting a data point does not change its semantic class. If the augmentation is too aggressive, the label *should* change. Forcing the model to output the same prediction destroys its learned representations.

32. **Question:** You build a Collaborative Filtering recommender system (Unsupervised/Self-Supervised matrix factorization).
*Twist:* A new user joins and clicks on three items. The system recommends them absolutely nothing relevant. Why?
**Answer:** The Cold Start problem.
**Mastery Explanation:** Pure collaborative filtering relies entirely on historical user-item interactions to find similar users/items. Without sufficient history, the latent vectors cannot be accurately updated. Content-based filtering is needed for cold starts.

33. **Question:** You train a Self-Supervised model using Masked Language Modeling (like BERT) by masking 15% of tokens.
*Twist:* You decide to mask 95% of the tokens to make the task "harder and learn better representations". The model fails to learn anything. Why?
**Answer:** Insufficient context.
**Mastery Explanation:** SSL requires predicting the hidden parts from the unhidden parts. If 95% is masked, there is not enough contextual signal left in the unhidden 5% to derive any meaningful grammatical or semantic relationships; the task becomes pure random guessing.

34. **Question:** You use a Generative Adversarial Network (GAN) to generate faces.
*Twist:* You train the Discriminator for 100 steps for every 1 step of the Generator to ensure the Discriminator is "perfect". The Generator completely stops learning. Why?
**Answer:** Vanishing Gradients.
**Mastery Explanation:** If the Discriminator is perfectly trained, it outputs probabilities extremely close to 0 or 1. The sigmoid cross-entropy loss function becomes completely flat in these regions, meaning no gradients flow back to the Generator to help it improve.

35. **Question:** You deploy a Multi-Armed Bandit (RL) algorithm using Epsilon-Greedy strategy to test ad click-through rates.
*Twist:* The click-through rates of the ads drift over time (non-stationary environment). The Epsilon-Greedy agent continues to exploit the initially best ad, ignoring the newly better ones. Why?
**Answer:** The standard sample-average action-value estimation does not discount old rewards.
**Mastery Explanation:** In a non-stationary environment, averaging all historical rewards means it takes extremely long for recent changes to shift the Q-values. The agent needs a constant step-size parameter (exponential recency-weighted average) to track non-stationary rewards.

36. **Question:** You are performing Weak Supervision using Snorkel, combining 10 different heuristic labeling functions.
*Twist:* All 10 labeling functions are highly accurate but they all check for the exact same keyword. The generative model fails to denoise effectively. Why?
**Answer:** High correlation/lack of conditional independence between labeling functions.
**Mastery Explanation:** Weak supervision models rely on modeling the agreements and disagreements between *diverse, conditionally independent* labeling sources to estimate their accuracies. If they all perfectly correlate, the model cannot distinguish between true signal and correlated noise.

37. **Question:** You are training a Multi-Task Learning (MTL) model with a shared backbone and two task-specific heads. Task A has a loss magnitude of 1000, Task B has a loss magnitude of 0.1.
*Twist:* You sum the losses as `L = L_A + L_B`. Task B performance is equivalent to random guessing. Why?
**Answer:** Gradient domination by Task A.
**Mastery Explanation:** Because Task A's loss is orders of magnitude larger, its gradients completely dominate the updates in the shared backbone. The network practically ignores Task B. You must use loss scaling, gradient normalization, or uncertainty weighting.

38. **Question:** You use a standard K-Means clustering algorithm (Unsupervised) on a dataset of customer locations.
*Twist:* The customer locations form concentric circles (e.g., a city center ring and a suburban ring). K-Means performs terribly. Why?
**Answer:** K-Means assumes isotropic, convex (spherical) clusters.
**Mastery Explanation:** K-Means partitions space using Voronoi cells based on Euclidean distance to centroids. It cannot model non-linear, non-convex manifold shapes like concentric rings. DBSCAN or Spectral Clustering must be used instead.

39. **Question:** You train a Support Vector Machine (Supervised) with an RBF kernel. It achieves 99% accuracy on the test set.
*Twist:* You inspect the model and find that the number of support vectors is equal to 99% of the training dataset size. What is the problem?
**Answer:** Severe Overfitting.
**Mastery Explanation:** Support vectors are the data points that define the decision boundary. If almost every point is a support vector, the model has simply memorized the training data by placing a tight Gaussian bump around every single instance (gamma is too high).

40. **Question:** You train an ensemble of Decision Trees (Random Forest) for regression.
*Twist:* You pass in test data containing feature values 2x larger than anything seen in the training data. The model predicts a flat horizontal line for all these points. Why?
**Answer:** Trees cannot extrapolate.
**Mastery Explanation:** Decision trees partition the feature space. Any value greater than the maximum split threshold in the training data simply falls into the outermost leaf node and receives the average target value of that node. They do not learn linear trends to extrapolate outside the training domain.

## Part 4: Coding & Debugging Questions (10)

41. **Question:** Look at this PyTorch pseudocode for self-supervised contrastive learning (SimCLR):
```python
def contrastive_loss(z_i, z_j, temperature):
    batch_size = z_i.shape[0]
    z_combined = torch.cat([z_i, z_j], dim=0)
    similarity_matrix = cosine_sim(z_combined, z_combined) / temperature
    
    # Target labels: z_i matches z_j
    labels = torch.cat([torch.arange(batch_size) + batch_size, torch.arange(batch_size)])
    loss = CrossEntropyLoss(similarity_matrix, labels)
    return loss
```
**Bug:** The similarity matrix includes the similarity of a vector with itself (the diagonal is 1.0), which dominates the softmax.
**Fix/Mastery Explanation:** You must mask out the self-similarity (the main diagonal) of the similarity matrix before applying the CrossEntropyLoss, otherwise the model minimizes the loss trivially by making the representation match itself perfectly (which it always does), ignoring the positive pairs.

42. **Question:** In an offline Reinforcement Learning script using PyTorch, you update the Q-network using TD-error:
```python
with torch.no_grad():
    target_q = reward + gamma * target_net(next_state).max(1)[0]
current_q = q_net(state).gather(1, action)
loss = mse_loss(current_q, target_q)
loss.backward()
optimizer.step()
```
**Bug:** The `done` (terminal state) mask is missing.
**Fix/Mastery Explanation:** If `next_state` is a terminal state, there is no future reward. The target calculation must be `target_q = reward + gamma * target_net(next_state).max(1)[0] * (1 - done)`. Without this, the value function diverges because it expects infinite future rewards from terminal states.

43. **Question:** You are writing a custom training loop for a Multi-Task Learning model.
```python
out_A, out_B = model(x)
loss_A = criterion_A(out_A, y_A)
loss_B = criterion_B(out_B, y_B)
loss_A.backward()
loss_B.backward()
optimizer.step()
```
**Bug:** Standard PyTorch `.backward()` clears the computational graph by default.
**Fix/Mastery Explanation:** Calling `loss_A.backward()` frees the graph for the shared backbone. When `loss_B.backward()` is called, it will throw a "RuntimeError: Trying to backward through the graph a second time". You must use `(loss_A + loss_B).backward()` or `loss_A.backward(retain_graph=True)`.

44. **Question:** You are writing a script to do Semi-Supervised pseudo-labeling.
```python
for unlabeled_batch in unlabeled_loader:
    preds = model(unlabeled_batch)
    pseudo_labels = torch.argmax(preds, dim=1)
    
    # Train on pseudo labels
    out = model(unlabeled_batch)
    loss = CrossEntropy(out, pseudo_labels)
    loss.backward()
```
**Bug:** Gradient flows through the pseudo-label generation.
**Fix/Mastery Explanation:** `preds` is part of the computational graph. When you generate `pseudo_labels`, you must detach it. Furthermore, computing pseudo-labels and then immediately training on the exact same outputs in the same pass provides essentially zero useful gradient (or causes graph issues). `pseudo_labels` should be created within a `torch.no_grad()` context.

45. **Question:** You implement Actor-Critic RL.
```python
policy_dist, value = model(state)
action = policy_dist.sample()
log_prob = policy_dist.log_prob(action)

advantage = reward - value
actor_loss = -(log_prob * advantage).mean()
critic_loss = advantage.pow(2).mean()

total_loss = actor_loss + critic_loss
total_loss.backward()
```
**Bug:** The advantage term in the actor loss allows gradients to flow back into the Critic.
**Fix/Mastery Explanation:** `advantage` is computed using `value`, which comes from the Critic. When computing `actor_loss = -(log_prob * advantage)`, gradients will flow through `advantage` into the Critic, destabilizing it. You must detach the advantage for the actor loss: `actor_loss = -(log_prob * advantage.detach()).mean()`.

46. **Question:** You are implementing a Federated Learning averaging script on the server.
```python
global_weights = server_model.state_dict()
for client_weights in client_updates:
    for key in global_weights.keys():
        global_weights[key] += client_weights[key] / len(client_updates)
server_model.load_state_dict(global_weights)
```
**Bug:** This approach incorrectly compounds the weights instead of properly averaging them relative to the previous state, leading to scale explosion if `global_weights` is not zeroed out first.
**Fix/Mastery Explanation:** You must initialize a zeroed dictionary for the new global weights, OR do `new_global_weights[key] = sum(client_weights[key]) / num_clients`. The code above adds the average of the client updates *on top of* the existing global weights, effectively doubling the magnitude of the weights every round.

47. **Question:** In an active learning sampling function, you want to find the top K most uncertain samples using entropy.
```python
probs = torch.softmax(model(unlabeled_pool), dim=1)
entropy = -torch.sum(probs * torch.log(probs), dim=1)
top_k_indices = torch.topk(entropy, K, largest=False)[1]
```
**Bug:** `largest=False` selects the lowest entropy points.
**Fix/Mastery Explanation:** Active learning targets the *most* uncertain points. High entropy means high uncertainty (the model's distribution is flat). By setting `largest=False`, you are querying the points the model is *most confident* about, completely defeating the purpose of active learning.

48. **Question:** You are writing an anomaly detection script using an Autoencoder (Unsupervised).
```python
def get_anomalies(data):
    reconstructed = autoencoder(data)
    mse = torch.mean((data - reconstructed)**2, dim=1)
    return mse < threshold
```
**Bug:** The condition `mse < threshold` marks normal data as anomalies.
**Fix/Mastery Explanation:** Autoencoders are trained to reconstruct normal data well. Therefore, normal data has a *low* reconstruction error. Anomalous data, which the model has not seen, will have a *high* reconstruction error. The correct logic is `mse > threshold`.

49. **Question:** Transfer Learning: You freeze the backbone of a ResNet but forget to set the batch normalization layers to eval mode.
```python
backbone.requires_grad_(False)
# backbone.eval() # FORGOT THIS
out = backbone(x)
```
**Bug:** Batch Norm statistics will continue to update during training.
**Fix/Mastery Explanation:** Even if `requires_grad=False` stops weight updates, Batch Normalization layers in `train()` mode will continue to update their running mean and variance based on the new target dataset batches. This destroys the pre-trained statistics and drastically degrades performance. You must explicitly call `backbone.eval()`.

50. **Question:** In a Zero-Shot classification setup using CLIP (Contrastive Language-Image Pretraining).
```python
image_features = clip_model.encode_image(images)
text_features = clip_model.encode_text(prompts)
logits = image_features @ text_features.T
probs = torch.softmax(logits, dim=1)
```
**Bug:** The features are not normalized before computing the dot product.
**Fix/Mastery Explanation:** CLIP is trained using cosine similarity, which requires the vectors to be L2-normalized. Without normalizing `image_features` and `text_features`, the dot product computes the unscaled inner product, where vector magnitude dominates the similarity score, completely ruining the zero-shot classification accuracy.
