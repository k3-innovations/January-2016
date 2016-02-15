package Assignment;

public class SumAndAverage {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int sum=0;
		for (int arrCnt=0;arrCnt<=100;arrCnt++){
			sum+=arrCnt;
		}
		
		//Made a comment change...
		System.out.println("Sum = " + sum);
		System.out.println("Average = " + new Double(sum/100));
		
	}

}
