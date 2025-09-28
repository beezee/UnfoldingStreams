namespace UnfoldingStreams;

public interface NaturalTransformation<F, G>
{
  public abstract K<G, A> Transform<A>(K<F, A> fa);
}