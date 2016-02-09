package Assignment;

import java.util.InputMismatchException;
import java.util.Scanner;

public class CheckPassFail {
	
	private static Scanner scan;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int mark = 1;
		
		while (mark != 0){ //value of 0 ends the program...
			scan = new Scanner(System.in);
			System.out.println("Enter score for Student: ");
		
			try{
				//get the mark/score
				mark = scan.nextInt();
				
				if (mark >= 50)
					System.out.println("PASS");
				else
					System.out.println("FAIL");
			}
			catch (InputMismatchException exception){
				System.out.println("Scre entered is not an integer. Invalid...try again");
			}
		}
	}

}
