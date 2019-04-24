def round_controlled(cycled_iterable, rounds=1):
    """Return after <rounds> passes through a cycled iterable."""
    round_start = None
    rounds_completed = 0

    for item in cycled_iterable:
        if round_start is None:
            round_start = item
        elif item == round_start:
            rounds_completed += 1

        if rounds_completed == rounds:
            return

        yield item
