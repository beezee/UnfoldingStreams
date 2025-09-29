using Xunit;

namespace UnfoldingStreams.Tests;

public record MyEitherIO<E, A>(EitherT<E, IO, A> Run): K<MyEitherIO<E>, A>;

public static class MyEitherIOExtensions
{
  public static MyEitherIO<E, A> As<E, A>(this K<MyEitherIO<E>, A> ma) =>
    (MyEitherIO<E, A>)ma;
}

public partial class MyEitherIO<E>:
Monad<MyEitherIO<E>>,
MonadIO<MyEitherIO<E>>,
Alternative<MyEitherIO<E>>
{
  public static K<MyEitherIO<E>, B> Bind<A, B>(K<MyEitherIO<E>, A> ma, Func<A, K<MyEitherIO<E>, B>> f) =>
    new MyEitherIO<E, B>(ma.As().Run.Bind(x => f(x).As().Run));

  public static K<MyEitherIO<E>, A> Pure<A>(A a) =>
    new MyEitherIO<E, A>(EitherT.Right<E, IO, A>(a));

  public static K<MyEitherIO<E>, B> Apply<A, B>(K<MyEitherIO<E>, Func<A, B>> mf, K<MyEitherIO<E>, A> ma) =>
    new MyEitherIO<E, B>(mf.As().Run.Apply(ma.As().Run));

  public static K<MyEitherIO<E>, B> Map<A, B>(Func<A, B> f, K<MyEitherIO<E>, A> ma) =>
    new MyEitherIO<E, B>(ma.As().Run.Map(f));

  public static K<MyEitherIO<E>, A> LiftIO<A>(IO<A> io) =>
    new MyEitherIO<E, A>(EitherT<E, IO, A>.LiftIO(io));

  public static K<MyEitherIO<E>, A> Choose<A>(K<MyEitherIO<E>, A> ma, Func<K<MyEitherIO<E>, A>> mb) =>
    new MyEitherIO<E, A>(ma.As().Run.Choose(() => mb().As().Run).As());

  public static K<MyEitherIO<E>, A> Choose<A>(K<MyEitherIO<E>, A> ma, K<MyEitherIO<E>, A> mb) =>
    Choose(ma, () => mb);

  public static K<MyEitherIO<E>, A> Empty<A>() =>
    new MyEitherIO<E, A>(EitherT<E, IO, A>.Left(default!));

  public static K<MyEitherIO<E>, A> Combine<A>(K<MyEitherIO<E>, A> ma, K<MyEitherIO<E>, A> mb) =>
    Choose(ma, () => mb);
}

public record MapLeft<E1, E2>(Func<E1, E2> Map)
: NaturalTransformation<Either<E1>, Either<E2>>
{
  public K<Either<E2>, A> Transform<A>(K<Either<E1>, A> fa) =>
    fa.As().MapLeft(Map);
}

public record MyEitherIOMapLeft<E1, E2>(MapLeft<E1, E2> MapLeft)
: NaturalTransformation<MyEitherIO<E1>, MyEitherIO<E2>>
{
  public K<MyEitherIO<E2>, A> Transform<A>(K<MyEitherIO<E1>, A> fa) =>
    new MyEitherIO<E2, A>(fa.As().Run.MapLeft(MapLeft.Map));
}

public class Tests
{
  [Fact]
  public void TakeTest()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var result = unfold.Take(5).Source().Collect().Run().AsEnumerable();
    Assert.Equal([1, 2, 3, 4, 5], result);
  }

  [Fact]
  public void SkipTest()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var result = unfold.Skip(5).Take(10).Source().Collect().Run().AsEnumerable();
    Assert.Equal([6, 7, 8, 9, 10, 11, 12, 13, 14, 15], result);

    var count2 = Atom(0);
    var unfold2 = Unfold.foreverM(() => swapIO(count2, c => c + 1));
    var result2 = unfold2.Take(10).Skip(5).Source().Collect().Run().AsEnumerable();
    Assert.Equal([6, 7, 8, 9, 10], result2);
  }
  
  [Fact]
  public void FilterTest()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var result = unfold.Filter(x => x % 2 == 0).Take(10).Source().Collect().Run().AsEnumerable();
    Assert.Equal([2, 4, 6, 8, 10, 12, 14, 16, 18, 20], result);

    var count2 = Atom(0);
    var unfold2 = Unfold.foreverM(() => swapIO(count2, c => c + 1));
    var result2 = unfold2.Take(10).Filter(x => x % 2 == 0).Source().Collect().Run().AsEnumerable();
    Assert.Equal([2, 4, 6, 8, 10], result2);
  }

  [Fact]
  public void EmptyTest()
  {
    var unfold = Unfold.empty<IO, int>();
    var result = unfold.Take(5).Source().Collect().Run().AsEnumerable();
    Assert.Empty(result);
  }


  [Fact]
  public void TransformTest()
  {
    var unfold = Unfold.foreverM(() => new MyEitherIO<string, int>(EitherT.Left<string, IO, int>("Hello")));
    var transformed = unfold.Transform(new MyEitherIOMapLeft<string, int>(new MapLeft<string, int>(s => s.Length)));
    var result = transformed.Take(5).Source().Collect().As().Run.Run().Run();
    Assert.Equal(Left<int, Seq<int>>(5), result);
  }

  [Fact]
  public void GroupedTest()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var result = unfold.Take(24).Grouped(5).Source().Collect().As().Run();
    Assert.Equal([
      [1, 2, 3, 4, 5],
      [6, 7, 8, 9, 10],
      [11, 12, 13, 14, 15],
      [16, 17, 18, 19, 20],
      [21, 22, 23, 24]], result);
  }

  [Fact]
  public void GroupedTest2()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var result = unfold.Grouped(3).Take(4).Source().Collect().As().Run();
    Assert.Equal([
      [1, 2, 3],
      [4, 5, 6],
      [7, 8, 9],
      [10, 11, 12]], result);
  }

  [Fact]
  public void TakeWhileTest()
  {
    var unfold = Unfold.fromSeq<IO, string>(["duck", "duck", "goose", "duck"]);
    var result = unfold.TakeWhile(x => x == "duck").Source().Collect().As().Run();
    Assert.Equal(["duck", "duck"], result);
  }

  [Fact]
  public void ZipTest()
  {
    var count = Atom(0);
    var unfold1 = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var unfold2 = Unfold.fromSeq<IO, string>(["a", "b", "c", "d", "e"]);
    var result = unfold1.Zip(unfold2).Source().Collect().As().Run();
    Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], result);
  }

  [Fact]
  public void ConcatTest()
  {
    var count = Atom(0);
    var unfold1 = Unfold.foreverM(() => swapIO(count, c => c + 1))
      .Take(5).Grouped(2);
    var unfold2 = Unfold.foreverM(() => swapIO(count, c => c + 1))
      .Grouped(2).Take(3);
    var result = unfold1.Concat(unfold2).Source().Collect().As().Run();
    Assert.Equal([[1, 2], [3, 4], [5], [6, 7], [8, 9], [10, 11]], result);
  }

  [Fact]
  public void TimedTest()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() =>
      from c in valueIO(count)
      from _ in IO.yieldFor(new Duration(c + 1))
      from upd in swapIO(count, c => c + 1)
      select upd);
    var res = unfold
      .GroupedWithin(
        // Illustrate timeout before any emit
        Schedule.fixedInterval(new Duration(2)).Take(1) +
        Schedule.fixedInterval(new Duration(20)).Take(3) +
        Schedule.fixedInterval(new Duration(50)),
        5)
      .Take(7).Source().Collect().As().Run();
    Console.WriteLine(res);
    var result = res.Map(xs => xs.Emit);
    // Timing assertion:
    // excluding first element for cold start
    // init of each sums to under schedule
    // sum of each is > elapsed - 5ms, < elapsed
    Assert.Equal(
      [
        [1],
        [2, 3, 4, 5, 6],
        [7, 8, 9],
        [10, 11],
        [12, 13, 14, 15],
        [16, 17, 18],
        [19, 20, 21]
      ],
      result
    );
  }

  [Fact]
  public void BindTest()
  {
    var count = Atom(0);
    var result = (
      from x in Unfold.foreverM(() => swapIO(count, c => c + 1)).Take(5).Source()
      from y in Unfold.cycle<IO, (int, int)>([(x, x + 1), (x + 2, x + 3)]).Take(3).Skip(1).Source()
      from z in Unfold.foreverM(() => IO.pure($"{y}")).Take(1).Source()
      select z
      ).As().Collect().Run();
    Assert.Equal([
      "(3, 4)", "(1, 2)",
      "(4, 5)", "(2, 3)",
      "(5, 6)", "(3, 4)",
      "(6, 7)", "(4, 5)",
      "(7, 8)", "(5, 6)"], result);
  } 

  [Fact]
  public void FilterMapTest()
  {
    var count = Atom(0);
    var unfold = Unfold.foreverM(() => swapIO(count, c => c + 1));
    var result = unfold.FilterMap(x => x % 2 == 0 ? Some($"{x}") : None).Take(5).Source().Collect().As().Run();
    Assert.Equal(["2", "4", "6", "8", "10"], result);
  }
}
