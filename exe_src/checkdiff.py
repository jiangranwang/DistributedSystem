import sys

def check_diff(file_1, file_2):
    # read from file
    f1 = open(file_1, 'r')
    f1_content = f1.read()
    f2 = open(file_2, 'r')
    f2_content = f2.read()

    # check for line numbers
    f1_lines = f1_content.split("\n")[:-1]
    f2_lines = f2_content.split("\n")[:-1]
    if len(f1_lines) != len(f2_lines):
        print(file_1 + "and" + file_2 + "are different: different line number!")
        return
    
    # check for key and value
    for i in range(len(f1_lines)):
        f1_line_arr = f1_lines[i].split(",")
        f2_line_arr = f2_lines[i].split("\t")
        f1_key, f1_value = f1_line_arr[0], f1_line_arr[1]
        f2_key, f2_value = f2_line_arr[0], f2_line_arr[1]
        if f1_key != f2_key:
            print(file_1 + " and " + file_2 + " are different: \n" + "Line:" + str(i) + "; different key:" + f1_key + f2_key)
            return
        if f1_value != f2_value:
            print(file_1 + " and " + file_2 + " are different: \n" + "Line:" + str(i) + "; different value:" + f1_value + f2_value)
            return
    print("two files are the same!")
    

if __name__ == "__main__":
    if len(sys.argv) <= 2:
        print("Needs Two Files to Compare!")
    else:
        file_1 = sys.argv[1]
        file_2 = sys.argv[2]
        print("Two files are: " + file_1, " and " + file_2)
        check_diff(file_1, file_2)
    