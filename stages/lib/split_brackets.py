from __pypy__.builders import UnicodeBuilder


def split_brackets(line):
    start = None
    in_quotes = False
    is_escaped = False

    num = 0
    line_len = len(line)
    ret = []
    accumulator = UnicodeBuilder()

    while num + 1 <= line_len:
        char = line[num]
        #print "Char %s: %s, %s" % (char, in_quotes, is_escaped)

        if char == "\\":
            if is_escaped:
                is_escaped = False
            else:
                is_escaped = True
                num += 1  # Skip the escape characters
                continue

        elif char == "'":
            if not is_escaped:
                in_quotes = not in_quotes
            else:
                is_escaped = False

        elif is_escaped:
            # Sometimes other characters are escaped.
            is_escaped = False

        if not in_quotes:
            if char == ",":
                char = "|"  # replace commas with |

            elif char == "(":
                if start is None:
                    start = num

            elif char == ")":
                ret.append(accumulator.build()[1:])
                accumulator = UnicodeBuilder()
                start = None

        num += 1
        if start:
            #print "Adding char %s" % char
            accumulator.append(char)

    return ret