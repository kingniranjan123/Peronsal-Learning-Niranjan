# Optimization Algorithms

**The underlying mathematical engines, such as Gradient Descent and L-BFGS, used to find the optimal weights that minimize the cost function of a machine learning model.**

## Why It Matters
When you call `.fit()` on a Spark ML estimator, an optimization algorithm runs distributed across your cluster to find the best model parameters. Understanding these algorithms is crucial because they determine how fast your model trains and whether it converges to a good solution at all. If your training job is hanging, taking forever, or returning terrible results, the problem often lies in how the optimization algorithm is configured (e.g., learning rate, max iterations).

## How It Works
Optimization algorithms navigate the "landscape" of the cost function to find the lowest point (the minimum).

1.  **Gradient Descent**: The standard approach. It computes the gradient (the slope) of the cost function and takes a step in the opposite direction.
    *   **Stochastic Gradient Descent (SGD)**: Approximates the gradient using a single data point.
    *   **Mini-Batch Gradient Descent**: Approximates the gradient using a small batch of data. This is what modern deep learning and many Spark ML algorithms use.
    *   *Momentum*: A technique added to gradient descent to accelerate it in the relevant direction and dampen oscillations, acting like a ball rolling down a hill.

2.  **L-BFGS (Limited-memory Broyden–Fletcher–Goldfarb–Shanno)**: This is a quasi-Newton method. While gradient descent only looks at the first derivative (slope), L-BFGS approximates the second derivative (curvature) of the cost function. 
    *   Because it understands curvature, it can take smarter, more direct steps towards the minimum. 
    *   It is the default optimizer for `LinearRegression` and `LogisticRegression` in Spark MLlib. It is generally much faster and more stable than SGD for linear models, though it requires more memory per step.

**Convergence Criteria**:
Optimizers don't run forever. They stop based on criteria you set:
*   `maxIter`: The maximum number of passes over the data.
*   `tol` (Tolerance): If the change in the cost function between iterations is smaller than this value, the algorithm assumes it has found the minimum and stops (converges).

## Flow Diagram
```
# Architecture Diagram
# (See MD source for diagram code)
graph TD
    A[Start Optimization] --> B[Compute Gradients / Curvature]
    B --> C[Update Model Weights]
    C --> D{Check Convergence}
    D -- "Delta < Tolerance (tol)" --> E[Stop: Converged]
    D...
```

## Data Visualization
**Monitoring Convergence (Objective History)**
You can extract the `objectiveHistory` from a trained Spark model to see how the cost function decreased over time.

| Iteration | Objective (Cost) | Note |
| :--- | :--- | :--- |
| 1 | 0.852 | Initial high error |
| 2 | 0.412 | Rapid descent |
| 5 | 0.201 | Slowing down |
| 10 | 0.155 | Nearing minimum |
| 11 | 0.154 | Delta < `tol`, algorithm converges |

## Code Example
```python
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Optimization").getOrCreate()
# Assume 'df' is a prepared DataFrame with 'features' and 'label'
# (Using dummy setup for syntax demonstration)
df = spark.createDataFrame([(1.0, Vectors.dense([0.5, 0.1])), (0.0, Vectors.dense([0.1, 0.8]))], ["label", "features"])

# Configuring the Optimizer
lr = LogisticRegression(
    maxIter=100,       # Maximum number of iterations
    tol=1e-6,          # Convergence tolerance (stop if change is smaller than this)
    regParam=0.1
)

model = lr.fit(df)

# Accessing the training summary to inspect the optimizer's performance
summary = model.summary

print(f"Total Iterations executed: {summary.totalIterations}")
print("Objective History (Cost per iteration):")
for iter_num, cost in enumerate(summary.objectiveHistory):
    print(f"Iteration {iter_num}: {cost}")
```

## Common Pitfalls
*   **Setting `maxIter` Too Low**: If the algorithm doesn't have enough iterations to reach the minimum, the model will be under-optimized. Always check if the model actually converged before `maxIter` was reached.
*   **Learning Rate Issues (if using SGD)**: If the step size is too large, the algorithm will bounce around and never converge (diverge). If it's too small, it will take an eternity to train. L-BFGS handles step sizes automatically, which is why it's preferred.
*   **Ignoring Unscaled Data**: Optimizers struggle heavily with elongated, unscaled cost function landscapes. Gradient descent will oscillate wildly if features aren't scaled (e.g., using `StandardScaler`).

## Key Takeaway
Spark MLlib utilizes advanced optimizers like L-BFGS under the hood to minimize cost functions efficiently; configuring `maxIter` and `tol` correctly ensures your models train fully without wasting cluster resources.
