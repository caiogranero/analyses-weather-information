package analyses;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class OrdinaryLeastSquares {
	
	private SimpleRegression simpleRegression;
	
	public OrdinaryLeastSquares(){
		// creating regression object, passing true to have intercept term
	    this.simpleRegression = new SimpleRegression(true);
	}

	public double getFutureX(double y) {		
		return ( y - this.simpleRegression.getIntercept() ) / this.simpleRegression.getSlope();
	}

	public double getFutureY(double x) {
		return ( this.simpleRegression.getIntercept() + this.simpleRegression.getSlope() * x );
	}

	public SimpleRegression getSimpleRegression() {
		return simpleRegression;
	}
}
