import java.util.Scanner;

public class GradeStats {
	public double avScr=0.;
	public int maxScr=0;
	public int minScr=0;
	public static Scanner scan;
	
	public void avrgScore(int[] arrScr){
		int uBound=arrScr.length;
		for (int cnt=0; cnt<uBound;cnt++)
			avScr+=arrScr[cnt];
		avScr/=uBound;
	}
	
	public void maxScore(int[] arrScr){
		int uBound=arrScr.length;
		int cnt=0;
		while (cnt<uBound){
				maxScr = Math.max(maxScr, arrScr[cnt]);
			    cnt+=1;
		}		
		
	}
	public void minScore(int[] arrScr){
		int uBound=arrScr.length;
		int cnt=0;
		maxScore(arrScr);
		minScr=maxScr;
		while (cnt<uBound){
				minScr = Math.min(minScr, arrScr[cnt]);
			    cnt+=1;
		}
		
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//prompt to instantiate grade array
		System.out.println("How many Scores would you be entering today? ");
		scan = new Scanner(System.in);
		int noOfScores = scan.nextInt();
		int theScores[] = new int[noOfScores];
		
		System.out.println("Begin Entering Scores");
		
		for (int cnt = 0; cnt<noOfScores;cnt++){
			theScores[cnt] =new Scanner(System.in).nextInt();
		}
		
		GradeStats gs = new GradeStats();
		gs.avrgScore(theScores);
		gs.maxScore(theScores);
		gs.minScore(theScores);
		System.out.println("Average Score : " + gs.avScr);
		System.out.println("Maximum Score : " + gs.maxScr);
		System.out.println("Minimum Score : " + gs.minScr);
		
		
	}

}
