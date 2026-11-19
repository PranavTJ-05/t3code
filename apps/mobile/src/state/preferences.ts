import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import { AsyncResult, Atom, type AtomRegistry } from "effect/reactivity";

import * as MobilePreferences from "../persistence/mobile-preferences";
import * as Runtime from "../lib/runtime";

interface OptimisticPreferences {
  readonly values: Partial<MobilePreferences.Preferences>;
  readonly versions: Partial<Record<keyof MobilePreferences.Preferences, number>>;
}

// useAtomSet treats bare functions as AsyncResult updates; wrap preference transforms in payloads.
type PreferencesUpdate =
  | Partial<MobilePreferences.Preferences>
  | {
      readonly transform: (
        current: MobilePreferences.Preferences,
      ) => Partial<MobilePreferences.Preferences>;
    };

type PreferencesAtoms = Pick<AtomRegistry.AtomRegistry, "get" | "set">;

/** Keep optimistic patches separate so hydration cannot overwrite pending writes. */
export function createMobilePreferencesState(
  runtime: Atom.AtomRuntime<MobilePreferences.MobilePreferencesStore>,
) {
  const storedPreferencesAtom = runtime
    .atom(
      MobilePreferences.MobilePreferencesStore.pipe(
        Effect.flatMap((store) => store.load),
        Effect.catch((error) =>
          Effect.logWarning("Could not load mobile preferences.", error).pipe(
            Effect.as<MobilePreferences.Preferences>({}),
          ),
        ),
      ),
    )
    .pipe(Atom.keepAlive, Atom.withLabel("mobile:preferences:stored"));

  const optimisticPatchAtom = Atom.make<OptimisticPreferences>({ values: {}, versions: {} }).pipe(
    Atom.keepAlive,
    Atom.withLabel("mobile:preferences:optimistic-patch"),
  );
  const confirmedPreferencesAtom = Atom.make<MobilePreferences.Preferences>({}).pipe(
    Atom.keepAlive,
    Atom.withLabel("mobile:preferences:confirmed"),
  );
  let nextPatchVersion = 0;

  const preferencesAtom = Atom.make((get) => {
    const stored = get(storedPreferencesAtom);
    const confirmed = get(confirmedPreferencesAtom);
    const optimistic = get(optimisticPatchAtom);
    return AsyncResult.map(stored, (preferences) => ({
      ...preferences,
      ...confirmed,
      ...optimistic.values,
    }));
  }).pipe(Atom.keepAlive, Atom.withLabel("mobile:preferences"));

  const settlePatch = (
    registry: PreferencesAtoms,
    patch: Partial<MobilePreferences.Preferences>,
    version: number,
  ) => {
    const optimistic = registry.get(optimisticPatchAtom);
    const values = { ...optimistic.values } as Record<string, unknown>;
    const versions = { ...optimistic.versions } as Record<string, unknown>;
    for (const key of Object.keys(patch) as Array<keyof MobilePreferences.Preferences>) {
      if (optimistic.versions[key] === version) {
        delete values[key];
        delete versions[key];
      }
    }
    registry.set(optimisticPatchAtom, {
      values: values as Partial<MobilePreferences.Preferences>,
      versions: versions as Partial<Record<keyof MobilePreferences.Preferences, number>>,
    });
  };

  // The optimistic patch lands when a save is requested, before the store is reached.
  const stage = (update: PreferencesUpdate, registry: PreferencesAtoms) => {
    const currentPreferences = registry.get(preferencesAtom);
    const patch =
      "transform" in update
        ? update.transform(
            AsyncResult.isSuccess(currentPreferences) ? currentPreferences.value : {},
          )
        : update;
    const version = ++nextPatchVersion;
    const current = registry.get(optimisticPatchAtom);
    const versions = { ...current.versions };
    for (const key of Object.keys(patch) as Array<keyof MobilePreferences.Preferences>) {
      versions[key] = version;
    }
    registry.set(optimisticPatchAtom, { values: { ...current.values, ...patch }, versions });
    return { patch, version } as const;
  };

  const persist = Effect.fn("MobilePreferencesState.persist")(function* (
    update: PreferencesUpdate,
    staged: ReturnType<typeof stage>,
    registry: PreferencesAtoms,
  ) {
    const store = yield* MobilePreferences.MobilePreferencesStore;
    const saved = yield* (
      "transform" in update ? store.update(update.transform) : store.savePatch(staged.patch)
    ).pipe(
      Effect.tapError(() => Effect.sync(() => settlePatch(registry, staged.patch, staged.version))),
    );
    registry.set(confirmedPreferencesAtom, saved);
    settlePatch(registry, staged.patch, staged.version);
    return saved;
  });

  const save = (update: PreferencesUpdate, registry: PreferencesAtoms) =>
    persist(update, stage(update, registry), registry);

  const updatePreferencesAtom = runtime
    .fn(
      (update: PreferencesUpdate, get) =>
        save(update, { get, set: (atom, value) => get.set(atom, value) }),
      // Storage serializes shared-blob updates; keep concurrent invocations alive to prevent cross-field interruption.
      { concurrent: true },
    )
    .pipe(Atom.keepAlive, Atom.withLabel("mobile:preferences:update"));

  // The atom above settles with whichever overlapping write finishes; a caller that acts on its
  // own save's outcome uses this command, which gives every call its own result.
  const savePreferences = createRuntimeCommand(runtime, {
    label: "mobile:preferences:save",
    execute: save,
  });

  return { preferencesAtom, updatePreferencesAtom, save, savePreferences } as const;
}

const mobilePreferencesState = createMobilePreferencesState(Runtime.appAtomRuntime);

export const mobilePreferencesAtom = mobilePreferencesState.preferencesAtom;
export const updateMobilePreferencesAtom = mobilePreferencesState.updatePreferencesAtom;
export const saveMobilePreferences = mobilePreferencesState.savePreferences;
export const saveMobilePreferencesEffect = mobilePreferencesState.save;
