using System.Diagnostics;

public interface WithTimeout<M>
where M : MonadIO<M>
{
  K<M, Option<A>> Run<A>(Func<K<M, A>> ma, TimeSpan timeout);

  public K<M, (Option<TimeSpan>, Option<A>)> RunTracked<A>(Func<K<M, A>> ma, TimeSpan timeout) => (
    from stopwatch in IO.lift(() => new Stopwatch())
    from _start in IO.lift(() => stopwatch.Start())
    from res in Run(ma, timeout)
    from _stop in IO.lift(() => stopwatch.Stop())
    select (timeout.Remaining(stopwatch.Elapsed), res));
}

public class TimedUnliftIO<M> : WithTimeout<M>
where M : MonadUnliftIO<M>
{
  public K<M, Option<A>> Run<A>(Func<K<M, A>> ma, TimeSpan timeout) => 
    ma().TimeoutIO(timeout).Map(Some).MapIO(x => 
      x.Catch(
        Predicate: e => e.Is(Errors.TimedOut) || e.Is(Errors.Cancelled),
        Fail: _ => IO.pure<Option<A>>(None)));
}

public class TimedHonorSystem<M> : WithTimeout<M>
where M : MonadIO<M>
{
  public K<M, Option<A>> Run<A>(Func<K<M, A>> ma, TimeSpan timeout) =>
    ma().Map(Some);
}

public static class WithTimeout
{
  public static WithTimeout<M> UnliftIO<M>()
  where M: MonadUnliftIO<M> => new TimedUnliftIO<M>();

  public static WithTimeout<M> HonorSystem<M>()
  where M: MonadIO<M> => new TimedHonorSystem<M>();
}

public static class ScheduleExtensions
{
  public static Option<TimeSpan> Remaining(this TimeSpan ts, TimeSpan spent)
  {
    Console.WriteLine($"ts: {ts}, spent: {spent}, remaining: {(ts > spent ? ts - spent : TimeSpan.Zero)}");
    return ts > spent ? Some(ts - spent) : None;
  }

  public static (Option<X>, Iterable<X>) Uncons<X>(this Iterable<X> xs) =>
    xs.IsEmpty() ? (None, xs) : (xs.Head(), xs.Tail());

  public static (Option<TimeSpan>, Iterable<Duration>) Pop(this Iterable<Duration> schedule) => (
    from durRest in Identity.Pure(schedule.Uncons())
    select (
      durRest.Item1.Map(
        x => TimeSpan.FromMilliseconds(x.Milliseconds)),
      durRest.Item2)
  ).As().Value;

  public static (Option<TimeSpan>, Iterable<Duration>) Pop(this Schedule schedule) =>
    schedule.Run().Pop();
}