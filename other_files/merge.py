# Python program to
# demonstrate merging of
# two files

# Creating a list of filenames
filenames = ['/Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomlist3M.txt', '/Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomlist3M.txt']

# Open file3 in write mode
with open('/Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomlist6M.txt', 'w') as outfile:
    # Iterate through list
    for names in filenames:
        # Open each file in read mode
        with open(names) as infile:
            # read the data from file1 and
            # file2 and write it in file3
            outfile.write(infile.read())

            # Add '\n' to enter data of file2
        # from next line
        outfile.write("\n")