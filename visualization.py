import matplotlib.pyplot as plt
import os

PLAN_DIR = "plans"

KEYWORDS = ["Project", "Filter", "Aggregate", "Exchange", "Join"]


def count_operations(plan_text):
    counts = {k: plan_text.count(k) for k in KEYWORDS}
    return counts


def load_and_analyze_plans():
    stats = {}
    for file in os.listdir(PLAN_DIR):
        if file.endswith(".txt"):
            name = file.replace("_plan.txt", "").replace("_physical", "")
            with open(os.path.join(PLAN_DIR, file)) as f:
                content = f.read()
                stats[name] = count_operations(content)
    return stats


def plot_plan_comparison(stats):
    for keyword in KEYWORDS:
        plt.figure()
        values = [v.get(keyword, 0) for v in stats.values()]
        plt.bar(stats.keys(), values, color='skyblue')
        plt.title(f"Count of '{keyword}' in Execution Plans")
        plt.ylabel("Count")
        plt.xlabel("Processor Type")
        plt.savefig(f"plan_{keyword.lower()}.png")
        plt.close()


if __name__ == "__main__":
    stats = load_and_analyze_plans()
    plot_plan_comparison(stats)