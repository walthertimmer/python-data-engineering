from resource import getrusage, RUSAGE_SELF

print("Peak memory (MiB):",
      int(getrusage(RUSAGE_SELF).ru_maxrss / 1024))