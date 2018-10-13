HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'


def header(s):
    return '{}{}{}'.format(HEADER, s, ENDC)


def okblue(s):
    return '{}{}{}'.format(OKBLUE, s, ENDC)


def okgreen(s):
    return '{}{}{}'.format(OKGREEN, s, ENDC)


def warning(s):
    return '{}{}{}'.format(WARNING, s, ENDC)


def fail(s):
    return '{}{}{}'.format(FAIL, s, ENDC)


def bold(s):
    return '{}{}{}'.format(BOLD, s, ENDC)


def underline(s):
    return '{}{}{}'.format(UNDERLINE, s, ENDC)

