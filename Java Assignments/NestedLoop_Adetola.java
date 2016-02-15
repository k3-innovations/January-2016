package Assignment;

import java.util.InputMismatchException;
import java.util.Scanner;

public class NestingClass {
	
	private static Scanner scan;
	private static int theNumber[] = {1,2,3,4,5,6,7,8,9};
	private static int scanNumber;
	private static String strVals[]={"ONE","TWO","THREE","FOUR","FIVE","SIX","SEVEN","EIGHT","NINE"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		while (true){
			System.out.println("Enter the number: ");
			scan = new Scanner(System.in);
			
			try{
				scanNumber = scan.nextInt();
				if (scanNumber > 9)
					System.out.println("OTHER");
				else
					for (int cnt=0; cnt<theNumber.length;cnt++){
						if (scanNumber ==theNumber[cnt])
							System.out.println(strVals[cnt]);
				}		
			}
			catch (InputMismatchException exception){
				System.out.println("INVALID INPUT");
			}

		}
		}
	

}
