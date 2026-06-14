def user_probability_from_model_score(raw_probability: float, threshold: float) -> float:
    """
    Tree ensembles can use a decision threshold far below 50%. For user-facing
    output, lift low-threshold "yes" scores into a readable 51%+ zone, but keep
    low-threshold "no" scores close to the raw model score.
    """
    raw_probability = min(max(raw_probability, 0.0), 1.0)
    threshold = min(max(threshold, 0.01), 0.99)

    if raw_probability >= threshold:
        scaled = raw_probability + max(0.0, 0.51 - threshold)
        return round(min(max(scaled, 0.51), 0.95), 4)

    if threshold <= 0.5:
        return round(min(raw_probability, 0.49), 4)

    scaled = (raw_probability / threshold) * 0.49

    return round(min(max(scaled, 0.05), 0.95), 4)
