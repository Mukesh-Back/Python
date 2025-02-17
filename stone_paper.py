import random
print(f"Welcome to our Game")
player1 = input("Enter Player name : ")
print("The Game is Begin")
player1_mark = 0
player2_mark = 0
def Game():
    global player1_mark
    global player2_mark
    for i in range(1, 4):
        check = random.randrange(1, 3 + 1)
        in1 = int(input(
                """Give the Input 
          1)Stone
          2)Paper
          3)Scissor
          === """
            ))
        print("Round ==", i)
        if (in1 == 1) and (check == 3):
            print("You put Stone and Machine put Scissor")
            print(f"So {player1} Get one mark")
            player1_mark += 1
        elif (in1 == 2) and (check == 1):
            print("You put Paper and Machine put Stone")
            print(f"So {player1} Get one mark")
            player1_mark += 1
        elif (in1 == 3) and (check == 2):
            print("You put Scissor and Machine put Paper")
            print(f"So {player1} Get one mark")
            player1_mark += 1
        elif in1 == check:
            print("Its Same Value")
        elif in1 < 4:
            print("Please Enter Valid Input")
        else:
            print(f"You put {in1} and Machine put {check}")
            print("You out")
            print(f"So Machine Get one mark")
            player2_mark += 1
print()
print("Result == ")
if player1_mark > player2_mark:
    print(
        f"{player1} Win the Match The score is {player1_mark} /n Machine Score is {player2_mark}"
    )
else:
    print(
        f"Machine Win the Match The Score is {player2_mark} /n {player1} score is {player1_mark}"
    )
Game()
Op = input("Can you Play Game Again ? if yes enter any value or Give Just Enter")
if Op:
    Game()
else:
    print("Thanks for Playing Game")