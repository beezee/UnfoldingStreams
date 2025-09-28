namespace UnfoldingStreams;

public record struct Unfold<S, M, T>(
  S Start,
  Func<S, K<M, (Option<S>, Option<T>)>> Step
) where M : MonadIO<M>, Alternative<M>;

public static class UnfoldExtensions
{

  public static SourceT<M, T> Source<S, M, T>(this Unfold<S, M, T> unfold)
  where M : MonadIO<M>, Alternative<M> => (
    from nxtHead in SourceT.liftM(unfold.Step(unfold.Start))
    let tail = nxtHead.Item1.Match(
      None: () => SourceT.empty<M, T>(),
      Some: s => new Unfold<S, M, T>(s, unfold.Step).Source()
    )
    from res in nxtHead.Item2.Match(
      Some: h => SourceT.pure<M, T>(h) + tail,
      None: () => tail)
    select res
  );

  public static Unfold<S, M, T> Take<S, M, T>(this Unfold<S, M, T> unfold, int n)
  where M : MonadIO<M>, Alternative<M> =>
    Unfold.take(unfold, n);

  public static Unfold<S, M, T> TakeWhile<S, M, T>(this Unfold<S, M, T> unfold, Func<T, bool> predicate)
  where M : MonadIO<M>, Alternative<M> =>
    Unfold.takeWhile(unfold, predicate);

  public static Unfold<(S, T), M, (A, B)> Zip<S, T, M, A, B>(this Unfold<S, M, A> l, Unfold<T, M, B> r)
  where M : MonadIO<M>, Alternative<M> =>
    Unfold.zip(l, r);

  public static Unfold<S, M, T> Skip<S, M, T>(this Unfold<S, M, T> unfold, int n)
  where M : MonadIO<M>, Alternative<M> =>
    Unfold.skip(unfold, n);

  public static Unfold<S, M, T> Filter<S, M, T>(this Unfold<S, M, T> unfold, Func<T, bool> predicate)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    Unfold.filter(unfold, predicate);

  public static Unfold<S, G, T> Transform<S, F, G, T>(this Unfold<S, F, T> unfold, NaturalTransformation<F, G> f)
  where F : MonadIO<F>, Alternative<F>
  where G : MonadIO<G>, Alternative<G> =>
    Unfold.transform(unfold, f);

  public static Unfold<(S, U), M, U> FoldUntil<S, M, T, U>(this Unfold<S, M, T> unfold, Func<U, T, U> folder, Func<U, bool> predicate, U initial)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    Unfold.foldUntil(unfold, folder, predicate, initial);

  public static Unfold<(S, Seq<T>), M, Seq<T>> Grouped<S, M, T>(this Unfold<S, M, T> unfold, int size)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    Unfold.grouped(unfold, size);

  public static Unfold<(S, TimeSpan, Iterable<Duration>), M, T> OnSchedule<S, M, T>(
    this Unfold<S, M, T> unfold,
    Schedule schedule,
    WithTimeout<M> timed,
    Func<S, (Option<S>, T)> onExpire
  ) where M : MonadIO<M>, Alternative<M>, Monad<M> =>
    Unfold.onSchedule(unfold, schedule, timed, onExpire);

  public static Unfold<(S, TimeSpan, Iterable<Duration>), M, T> OnSchedule<S, M, T>(
    this Unfold<S, M, T> unfold,
    Schedule schedule,
    Func<S, (Option<S>, T)> onExpire
  ) where M : MonadIO<M>, Alternative<M>, Monad<M>, MonadUnliftIO<M> =>
    Unfold.onSchedule(unfold, schedule, onExpire);

  public static Unfold<((S, Seq<T>), TimeSpan, Iterable<Duration>), M, Seq<T>> GroupedWithin<S, M, T>(
    this Unfold<S, M, T> unfold,
    Schedule schedule,
    WithTimeout<M> timed,
    int size
  ) where M : MonadIO<M>, Alternative<M>, Monad<M> =>
    Unfold.groupedWithin(unfold, schedule, timed, size);

  public static Unfold<((S, Seq<T>), TimeSpan, Iterable<Duration>), M, Seq<T>> GroupedWithin<S, M, T>(
    this Unfold<S, M, T> unfold,
    Schedule schedule,
    int size
  ) where M : MonadIO<M>, Alternative<M>, Monad<M>, MonadUnliftIO<M> =>
    Unfold.groupedWithin(unfold, schedule, size);

  public static Unfold<S, M, T> Concat<S, M, T>(this Unfold<S, M, T> fst, Unfold<S, M, T> snd)
  where M : MonadIO<M>, Alternative<M> =>
    Unfold.concat(fst, snd);
};

public static partial class Unfold
{
  public static Unfold<Unit, M, T> foreverMFiltered<M, T>(Func<K<M, Option<T>>> step)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    new Unfold<Unit, M, T>(
      unit,
      _ => step().Map(x => (Some(unit), x)));

  public static Unfold<Unit, M, T> foreverM<M, T>(Func<K<M, T>> step)
  where M : MonadIO<M>, Alternative<M> =>
    foreverMFiltered(() => step().Map(Some));

  public static Unfold<Seq<T>, M, T> fromSeq<M, T>(Seq<T> seq)
  where M : MonadIO<M>, Alternative<M> =>
    new Unfold<Seq<T>, M, T>(
      seq,
      rem => M.Pure(rem.Match(
        Empty: () => (None, None),
        Tail: (h, t) => (Some(t), Some(h)))));

  public static Unfold<S, M, T> empty<S, M, T>(S start)
  where M : MonadIO<M>, Alternative<M>, Monad<M> =>
    new Unfold<S, M, T>(
      start,
      _ => M.Pure<(Option<S>, Option<T>)>((None, None)));

  public static Unfold<Unit, M, T> empty<M, T>()
  where M : MonadIO<M>, Alternative<M>, Monad<M> =>
    empty<Unit, M, T>(unit);

  public static Unfold<(S, T), M, (A, B)> zip<S, M, T, A, B>(Unfold<S, M, A> l, Unfold<T, M, B> r)
  where M : MonadIO<M>, Alternative<M>, Applicative<M> =>
    new Unfold<(S, T), M, (A, B)>(
      (l.Start, r.Start),
      s => (l.Step(s.Item1), r.Step(s.Item2)).Apply(
        (lt, rt) => (
          (lt.Item1, rt.Item1).Apply((s1, s2) => (s1, s2)).As(),
          (lt.Item2, rt.Item2).Apply((a, b) => (a, b)).As())));

  public static Unfold<S, M, T> takeWhile<S, M, T>(Unfold<S, M, T> unfold, Func<T, bool> predicate)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    new Unfold<S, M, T>(
      unfold.Start,
      s => M.Map<(Option<S>, Option<T>), (Option<S>, Option<T>)>(
        t => t.Item2.Match(
          Some: h => Some(h).Filter(predicate).Match(
            Some: _ => t,
            None: () => (None, None)),
          None: () => t),
        unfold.Step(s)));

  public static Unfold<S, M, T> skip<S, M, T>(Unfold<S, M, T> unfold, int n)
  where M : MonadIO<M>, Alternative<M>, Functor<M>
  {
    var count = 0;
    return new Unfold<S, M, T>(
      unfold.Start,
      s => M.Map<(Option<S>, Option<T>), (Option<S>, Option<T>)>(
        t =>
        {
          if (count < n)
          {
            count = count + 1;
            return (t.Item1, None);
          }
          else
          {
            return t;
          }
        },
        unfold.Step(s)
      )
    );
  }

  public static Unfold<S, M, T> take<S, M, T>(Unfold<S, M, T> unfold, int n)
  where M : MonadIO<M>, Alternative<M>
  {
    var count = 1;
    return new Unfold<S, M, T>(
      unfold.Start,
      s => M.Map<(Option<S>, Option<T>), (Option<S>, Option<T>)>(
        t => t.Item2.Match(
          Some: _ =>
          {
            if (count < n)
            {
              count = count + 1;
              return (t.Item1, t.Item2);
            }
            else
            {
              return (None, t.Item2);
            }
          },
          None: () => t),
        unfold.Step(s)
      )
    );
  }

  public static Unfold<S, G, A> transform<S, F, G, A>(Unfold<S, F, A> unfold, NaturalTransformation<F, G> f)
  where G : MonadIO<G>, Alternative<G>
  where F : MonadIO<F>, Alternative<F> =>
    new Unfold<S, G, A>(
      unfold.Start,
      s => f.Transform(unfold.Step(s)));

  public static Unfold<S, M, T> filter<S, M, T>(Unfold<S, M, T> unfold, Func<T, bool> predicate)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    new Unfold<S, M, T>(
      unfold.Start,
      s => M.Map<(Option<S>, Option<T>), (Option<S>, Option<T>)>(
        t => (t.Item1, t.Item2.Filter(predicate)),
        unfold.Step(s)));

  public static Unfold<(S, U), M, U> foldUntil<S, M, T, U>(
    Unfold<S, M, T> unfold,
    Func<U, T, U> folder,
    Func<U, bool> predicate,
    U initial
  ) where M : MonadIO<M>, Alternative<M>, Monad<M> =>
    new Unfold<(S, U), M, U>(
      (unfold.Start, initial),
      su => unfold.Step(su.Item1).Map(st => (
        from nu in Identity.Pure(
          st.Item2.Match<U>(
            None: () => su.Item2,
            Some: t => folder(su.Item2, t)))
        select (
          st.Item1.Map(s => (s, predicate(nu) ? initial : nu)),
          Some(nu).Filter(x => st.Item1.IsNone || predicate(x))
        )
      ).As().Value));

  public static Unfold<(S, TimeSpan, Iterable<Duration>), M, T> onSchedule<S, M, T>(
    Unfold<S, M, T> unfold,
    Schedule schedule,
    WithTimeout<M> timed,
    Func<S, (Option<S>, T)> onExpire
  ) where M : MonadIO<M>, Alternative<M>, Monad<M> => (
    from sched in Identity.Pure(schedule.Pop())
    select sched.Item1.Match(
      None: () => Unfold.empty<(S, TimeSpan, Iterable<Duration>), M, T>(
        (unfold.Start, TimeSpan.Zero, sched.Item2)
      ),
      Some: ts => new Unfold<(S, TimeSpan, Iterable<Duration>), M, T>(
        (unfold.Start, ts, sched.Item2),
        s => (
          from res in timed.RunTracked(() => unfold.Step(s.Item1), s.Item2)
           .Map(x => { Console.WriteLine($"ts: {s.Item2}, res: {x}"); return x;  })
          let handleExpire = res.Item2
            .Filter(x => x.Item2.IsSome || res.Item1.IsSome)
            .ToEither<S>(
              // Edge case - if interval ends
              // no timeout, no emit, and signal terminate
              // we will flush from originating state
              // and proceed from flush output state
              // This means "bypass" termination
              // Alternatives exist with their own compromises
              res.Item2.Bind(x => x.Item1).IfNone(s.Item1)
            )
            .Match(
              Left: s => (
                from onExp in Identity.Pure(onExpire(s))
                select (
                  onExp.Item1,
                  Some(onExp.Item2)
                )
              ).As().Value,
              Right: t => t
            )
          let nextSched = res.Item1
            .Filter(_ => handleExpire.Item2.IsNone)
            .Match(
              Some: ts => Some((ts, s.Item3)),
              None: () => (
                from nxtSched in Identity.Pure(s.Item3.Pop())
                select nxtSched.Item1.Map(ts => (ts, nxtSched.Item2))
              ).As().Value
            )
          select 
            ((handleExpire.Item1, nextSched).Apply((s, ns) => (
              s, ns.Item1, ns.Item2
            )).As(), handleExpire.Item2)
        )
      )
    )
  ).As().Value;

  public static Unfold<(S, TimeSpan, Iterable<Duration>), M, T> onSchedule<S, M, T>(
    Unfold<S, M, T> unfold,
    Schedule schedule,
    Func<S, (Option<S>, T)> onExpire
  ) where M : MonadIO<M>, Alternative<M>, Monad<M>, MonadUnliftIO<M> =>
    onSchedule(unfold, schedule, WithTimeout.UnliftIO<M>(), onExpire);

  public static Unfold<(S, Seq<T>), M, Seq<T>> grouped<S, M, T>(Unfold<S, M, T> unfold, int size)
  where M : MonadIO<M>, Alternative<M>, Functor<M> =>
    foldUntil<S, M, T, Seq<T>>(
      unfold,
      (s, t) => s + [t],
      s => s.Count >= size,
      []
    );

  public static Unfold<((S, Seq<T>), TimeSpan, Iterable<Duration>), M, Seq<T>> groupedWithin<S, M, T>(
    Unfold<S, M, T> unfold,
    Schedule schedule,
    WithTimeout<M> timed,
    int size
  ) where M : MonadIO<M>, Alternative<M>, Monad<M> =>
    onSchedule<(S, Seq<T>), M, Seq<T>>(
      grouped(unfold, size),
      schedule,
      timed, s => (Some<(S, Seq<T>)>((s.Item1, [])), s.Item2));

  public static Unfold<((S, Seq<T>), TimeSpan, Iterable<Duration>), M, Seq<T>> groupedWithin<S, M, T>(
    Unfold<S, M, T> unfold,
    Schedule schedule,
    int size
  ) where M : MonadIO<M>, Alternative<M>, Monad<M>, MonadUnliftIO<M> =>
    groupedWithin(unfold, schedule, WithTimeout.UnliftIO<M>(), size);

  public static Unfold<S, M, T> concat<S, M, T>(Unfold<S, M, T> fst, Unfold<S, M, T> snd)
  where M : MonadIO<M>, Alternative<M>, Monad<M> {
    var finishedFst = false;
    return new Unfold<S, M, T>(
      fst.Start,
      s => finishedFst
        ? snd.Step(s)
        : fst.Step(s).Bind(t => t.Item1.Match(
          None: () =>
          {
            finishedFst = true;
            return M.Pure((Some(snd.Start), t.Item2));
          },
          Some: x => M.Pure((Some(x), t.Item2))
        )));
  }
}