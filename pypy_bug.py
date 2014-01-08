import random, string

big_dict = {"".join([random.choice(string.printable)
                     for i in xrange(40)]): random.randint(0,10000) for _ in xrange(10000000)}
valid_key = big_dict.keys()[0]
print "Valid key: %s" % big_dict[valid_key]
print "Invalid key: %s" % big_dict["test"]