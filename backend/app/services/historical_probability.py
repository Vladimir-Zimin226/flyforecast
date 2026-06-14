def user_probability_from_model_score(raw_probability: float, threshold: float) -> float:
    """
    Tree ensembles often produce useful ranking scores, but their decision threshold
    can be far below 50%. For user-facing output, map the learned threshold to 50%
    so "Да" and the displayed percentage mean the same thing.
    """
    raw_probability = min(max(raw_probability, 0.0), 1.0)
    threshold = min(max(threshold, 0.01), 0.99)

    if raw_probability >= threshold:
        scaled = 0.5 + ((raw_probability - threshold) / (1 - threshold)) * 0.45
    else:
        scaled = 0.05 + (raw_probability / threshold) * 0.45

    return round(min(max(scaled, 0.05), 0.95), 4)
