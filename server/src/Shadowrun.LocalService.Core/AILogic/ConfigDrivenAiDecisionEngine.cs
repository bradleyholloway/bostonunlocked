using System;
using System.Collections.Generic;
using Cliffhanger.SRO.ServerClientCommons;
using Cliffhanger.SRO.ServerClientCommons.GameLogic;
using Cliffhanger.SRO.ServerClientCommons.GameLogic.Components;
using Cliffhanger.SRO.ServerClientCommons.ArtificialIntelligence;
using Cliffhanger.SRO.ServerClientCommons.Gameworld;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.Locomotion;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.RandomNumbers;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.Skills;
using SRO.Core.Compatibility.Math;

namespace Shadowrun.LocalService.Core.AILogic
{
    /// <summary>
    /// Very small first-pass AI engine that demonstrates the intended runtime wiring:
    /// - resolve AI config for agent
    /// - build a skill selector from SkillRotation
    /// - pick a skill id
    ///
    /// Placeholder: does not do targeting/movement, and may return HasAction=false.
    /// </summary>
    public sealed class ConfigDrivenAiDecisionEngine : IAiDecisionEngine
    {
        private readonly IAiBehaviourConfigLookup _configLookup;
        private readonly ISkillSelectionStrategyFactory _skillSelectionFactory;
        private readonly IRandomNumberGenerator _rng;

        // Minimal per-agent memory to reduce jitter:
        // - stick to a chosen nearest enemy instead of re-picking every sub-step
        // - avoid immediately stepping back onto the previous tile
        private readonly Dictionary<int, int> _stickyEnemyByAgentId = new Dictionary<int, int>();
        private readonly Dictionary<int, IntVector2D> _lastMoveFromByAgentId = new Dictionary<int, IntVector2D>();
        private readonly Dictionary<int, IntVector2D> _lastMoveToByAgentId = new Dictionary<int, IntVector2D>();

        public ConfigDrivenAiDecisionEngine(
            IAiBehaviourConfigLookup configLookup,
            ISkillSelectionStrategyFactory skillSelectionFactory,
            IRandomNumberGenerator rng)
        {
            _configLookup = configLookup;
            _skillSelectionFactory = skillSelectionFactory;
            _rng = rng;
        }

        public AiDecision Decide(Entity agent, IGameworldInstance gameworld)
        {
            var decision = new AiDecision(agent);

            decision.DebugStage = "start";

            try
            {
                if (_configLookup == null || _skillSelectionFactory == null || gameworld == null)
                {
                    decision.DebugStage = "missing-deps";
                    return decision;
                }

                AIBehaviourConfigurationComponent config;
                if (!_configLookup.TryGetConfig(agent, gameworld, out config) || config == null)
                {
                    decision.DebugStage = "no-config";
                    return decision;
                }

                decision.DebugHasAiConfig = true;

                var rotation = config.SkillRotation;
                if (rotation == null)
                {
                    decision.DebugStage = "rotation-null";
                    return decision;
                }

                decision.DebugRotationType = rotation.GetType().FullName;
                decision.DebugRotationCount = TryGetRotationCount(rotation);

                // Gather loadout info for debugging (even if we end up not using it).
                try
                {
                    ISkillLoadoutComponent loadout;
                    if (gameworld.EntitySystem != null && gameworld.EntitySystem.TryGetComponent<ISkillLoadoutComponent>(agent, out loadout) && loadout != null)
                    {
                        decision.DebugHasLoadout = true;
                        decision.DebugSelectedWeaponIndex = loadout.SelectedWeaponIndex;
                        decision.DebugSelectedWeaponSkillCount = (loadout.SelectedWeapon != null && loadout.SelectedWeapon.Skills != null)
                            ? (int?)loadout.SelectedWeapon.Skills.Length
                            : null;
                    }
                }
                catch
                {
                    // Debug-only.
                }

                IAISkillSelection selector;
                try
                {
                    selector = rotation.CreateFor(_skillSelectionFactory, agent, _rng);
                }
                catch
                {
                    selector = null;
                }

                if (selector == null)
                {
                    decision.DebugStage = "selector-null";
                    return decision;
                }

                // Determine how to interpret SelectSkill(): some rotations are indices into the agent's loadout,
                // others may already be activity ids.
                var isIndexBased = rotation is RotationSkillSelection || rotation is WeightedSkillSelection || rotation is ConditionalRotationSkillSelection;

                // Try a few selections before giving up and moving. Some rotations include non-combat/self-buff
                // activities (e.g. AIRepairOwnArmor) that frequently produce no visible effect.
                var rotationCount = decision.DebugRotationCount.HasValue ? decision.DebugRotationCount.Value : 0;
                var maxAttempts = rotationCount > 0 ? Math.Min(8, rotationCount) : 4;

                // If we can't execute a skill right now, keep the last resolved activity so we can try
                // "attack-aware movement": move to a tile where that activity would actually have targets.
                var lastResolvedWeaponIndex = 0;
                var lastResolvedSkillIndex = 0;
                ulong lastResolvedActivityId = 0UL;

                for (var attempt = 0; attempt < maxAttempts; attempt++)
                {
                    ulong selected = 0UL;
                    try { selected = selector.SelectSkill(); }
                    catch { selected = selector.DefaultSkill; }

                    // Preserve the first selection for debugging, but keep later ones too.
                    if (attempt == 0)
                    {
                        decision.DebugRawSelection = selected;
                    }

                    // IMPORTANT: for index-based rotations a selection of 0 means "use skill slot 0" and is valid.
                    // Only treat 0 as invalid for activity-id based rotations.
                    if (!isIndexBased && selected == 0UL)
                    {
                        decision.DebugStage = "selected-zero";
                        break;
                    }

                    int? weaponIndex;
                    int? skillIndex;
                    ulong activityId;
                    if (!TryResolveActivityToRun(gameworld, agent, rotation, selected, out weaponIndex, out skillIndex, out activityId))
                    {
                        decision.DebugStage = "resolve-failed";
                        continue;
                    }

                    if (activityId != 0UL)
                    {
                        lastResolvedActivityId = activityId;
                        lastResolvedWeaponIndex = weaponIndex.HasValue ? weaponIndex.Value : 0;
                        lastResolvedSkillIndex = skillIndex.HasValue ? skillIndex.Value : 0;
                    }

                    // Pick a target position that would actually produce a target list.
                    IntVector2D chosenTarget;
                    if (TryPickValidTargetPosition(gameworld, agent, weaponIndex ?? 0, skillIndex ?? 0, activityId, out chosenTarget))
                    {
                        decision.DebugResolvedActivityId = activityId;
                        decision.TargetPosition = chosenTarget;
                        decision.HasAction = true;
                        decision.SkillId = activityId;
                        decision.WeaponIndex = weaponIndex;
                        decision.SkillIndex = skillIndex;
                        decision.DebugStage = "ok";
                        return decision;
                    }

                    decision.DebugResolvedActivityId = activityId;
                    decision.DebugStage = "no-target";
                }

                // Attack-aware movement: if we had a resolved activity but no valid target from the current tile,
                // try to move to a reachable tile from which the same activity would have targets.
                if (lastResolvedActivityId != 0UL)
                {
                    var setupPos = TryPickMoveTargetToEnableActivity(gameworld, agent, lastResolvedWeaponIndex, lastResolvedSkillIndex, lastResolvedActivityId);
                    if (setupPos.HasValue)
                    {
                        decision.IsMove = true;
                        decision.MoveTargetPosition = setupPos.Value;
                        decision.HasAction = true;
                        decision.DebugStage = "move-for-activity";
                        decision.DebugResolvedActivityId = lastResolvedActivityId;
                        return decision;
                    }
                }

                // No valid target for this activity right now. Spend movement toward the nearest enemy.
                // IMPORTANT: do not target the enemy's occupied cell directly; the pathfinder may treat that as
                // unreachable, and the client will then appear to ignore the move. Instead pick a reachable cell
                // that reduces distance to the nearest enemy.
                int? debugPreferredEnemyId;
                int? debugChosenEnemyId;
                string debugEnemyPick;
                string debugEnemyReason;
                int? debugEnemyX;
                int? debugEnemyY;
                int? debugEnemyCandidateCount;
                int? debugReachableCellCount;
                int? debugReducingCellCount;
                bool? debugAvoidedImmediateBacktrack;
                int? debugCurrentDist;
                int? debugChosenEnemyTeamId;
                bool? debugChosenEnemyTeamAi;
                ulong? debugChosenEnemyControlPlayerId;
                bool? debugChosenEnemyControlAi;
                bool? debugChosenEnemyIsPlayersPlayerCharacter;
                bool? debugChosenEnemyInteractiveObject;
                IntVector2D? moveTarget = TryPickMoveTargetTowardNearestEnemyWithMemory(
                    gameworld,
                    agent,
                    out debugPreferredEnemyId,
                    out debugChosenEnemyId,
                    out debugChosenEnemyTeamId,
                    out debugChosenEnemyTeamAi,
                    out debugChosenEnemyControlPlayerId,
                    out debugChosenEnemyControlAi,
                    out debugChosenEnemyIsPlayersPlayerCharacter,
                    out debugChosenEnemyInteractiveObject,
                    out debugEnemyPick,
                    out debugEnemyReason,
                    out debugEnemyX,
                    out debugEnemyY,
                    out debugEnemyCandidateCount,
                    out debugReachableCellCount,
                    out debugReducingCellCount,
                    out debugAvoidedImmediateBacktrack,
                    out debugCurrentDist);

                decision.DebugPreferredEnemyId = debugPreferredEnemyId;
                decision.DebugChosenEnemyId = debugChosenEnemyId;
                decision.DebugChosenEnemyTeamId = debugChosenEnemyTeamId;
                decision.DebugChosenEnemyTeamAi = debugChosenEnemyTeamAi;
                decision.DebugChosenEnemyControlPlayerId = debugChosenEnemyControlPlayerId;
                decision.DebugChosenEnemyControlAi = debugChosenEnemyControlAi;
                decision.DebugChosenEnemyIsPlayersPlayerCharacter = debugChosenEnemyIsPlayersPlayerCharacter;
                decision.DebugChosenEnemyInteractiveObject = debugChosenEnemyInteractiveObject;
                decision.DebugEnemyPick = debugEnemyPick;
                decision.DebugEnemyReason = debugEnemyReason;
                decision.DebugEnemyX = debugEnemyX;
                decision.DebugEnemyY = debugEnemyY;
                decision.DebugEnemyCandidateCount = debugEnemyCandidateCount;
                decision.DebugReachableCellCount = debugReachableCellCount;
                decision.DebugReducingCellCount = debugReducingCellCount;
                decision.DebugAvoidedImmediateBacktrack = debugAvoidedImmediateBacktrack;
                decision.DebugCurrentDistToEnemy = debugCurrentDist;

                if (moveTarget.HasValue)
                {
                    decision.IsMove = true;
                    decision.MoveTargetPosition = moveTarget.Value;
                    decision.HasAction = true;
                    decision.DebugStage = "move";
                    return decision;
                }

                decision.DebugStage = "no-target";
                return decision;
            }
            catch
            {
                decision.DebugStage = "exception";
                return decision;
            }
        }

        private static bool TryPickValidTargetPosition(IGameworldInstance gameworld, Entity agent, int weaponIndex, int skillIndex, ulong activityId, out IntVector2D chosen)
        {
            chosen = IntVector2D.Zero;
            try
            {
                if (gameworld == null || gameworld.ActivitySystem == null || gameworld.EntitySystem == null)
                {
                    return false;
                }

                var dryRunner = gameworld.ActivitySystem as IActivitySystemDryRunner;
                if (dryRunner == null)
                {
                    return false;
                }

                // Candidate positions: prefer enemy occupied cells (blocked positions) + their anchor positions.
                // If no enemy-position yields any available targets, fall back to self + ally positions (support skills).
                var myPosN = TryGetAgentPosition(gameworld, agent);
                var myPos = myPosN.HasValue ? myPosN.Value : IntVector2D.Zero;

                TeamComponent myTeam;
                if (!gameworld.EntitySystem.TryGetComponent<TeamComponent>(agent, out myTeam) || myTeam == null)
                {
                    return false;
                }

                var enemyCandidates = new List<IntVector2D>();
                var friendlyCandidates = new List<IntVector2D>();
                var seenEnemy = new Dictionary<string, bool>(StringComparer.Ordinal);
                var seenFriendly = new Dictionary<string, bool>(StringComparer.Ordinal);

                foreach (var other in gameworld.EntitySystem.GetAllEntities())
                {
                    if (other == agent)
                    {
                        continue;
                    }

                    TeamComponent otherTeam;
                    if (!gameworld.EntitySystem.TryGetComponent<TeamComponent>(other, out otherTeam) || otherTeam == null)
                    {
                        continue;
                    }

                    var targetList = otherTeam.TeamID == myTeam.TeamID ? friendlyCandidates : enemyCandidates;
                    var seen = otherTeam.TeamID == myTeam.TeamID ? seenFriendly : seenEnemy;

                    IPositionComponent pos;
                    if (gameworld.EntitySystem.TryGetComponent<IPositionComponent>(other, out pos) && pos != null)
                    {
                        AddCandidate(pos.GridPosition, targetList, seen);
                        if (pos.BlockedGridPositions != null)
                        {
                            foreach (var blocked in pos.BlockedGridPositions)
                            {
                                AddCandidate(blocked, targetList, seen);
                            }
                        }
                    }
                    else
                    {
                        // Fallback to extension method.
                        try { AddCandidate(gameworld.EntitySystem.GetAgentGridPosition(other), targetList, seen); }
                        catch { }
                    }
                }

                // Sort by distance.
                enemyCandidates.Sort(delegate (IntVector2D a, IntVector2D b)
                {
                    var da = Math.Abs(a.X - myPos.X) + Math.Abs(a.Y - myPos.Y);
                    var db = Math.Abs(b.X - myPos.X) + Math.Abs(b.Y - myPos.Y);
                    return da.CompareTo(db);
                });

                for (var i = 0; i < enemyCandidates.Count; i++)
                {
                    var pos = enemyCandidates[i];
                    ActivityEvaluationResult eval;
                    try
                    {
                        eval = dryRunner.DryRunActivity(weaponIndex, skillIndex, activityId, agent, pos);
                    }
                    catch
                    {
                        eval = null;
                    }

                    if (eval != null && eval.SkillWasSuccessful && eval.TargetWorkspaces != null && eval.TargetWorkspaces.AvailableTargets > 0)
                    {
                        chosen = pos;
                        return true;
                    }
                }

                // Friendly/self fallback.
                if (myPosN.HasValue)
                {
                    AddCandidate(myPosN.Value, friendlyCandidates, seenFriendly);
                }

                friendlyCandidates.Sort(delegate (IntVector2D a, IntVector2D b)
                {
                    var da = Math.Abs(a.X - myPos.X) + Math.Abs(a.Y - myPos.Y);
                    var db = Math.Abs(b.X - myPos.X) + Math.Abs(b.Y - myPos.Y);
                    return da.CompareTo(db);
                });

                for (var i = 0; i < friendlyCandidates.Count; i++)
                {
                    var pos = friendlyCandidates[i];
                    ActivityEvaluationResult eval;
                    try
                    {
                        eval = dryRunner.DryRunActivity(weaponIndex, skillIndex, activityId, agent, pos);
                    }
                    catch
                    {
                        eval = null;
                    }

                    if (eval != null && eval.SkillWasSuccessful && eval.TargetWorkspaces != null && eval.TargetWorkspaces.AvailableTargets > 0)
                    {
                        chosen = pos;
                        return true;
                    }
                }

                return false;
            }
            catch
            {
                chosen = IntVector2D.Zero;
                return false;
            }
        }

        private static void AddCandidate(IntVector2D pos, List<IntVector2D> list, IDictionary<string, bool> seen)
        {
            var key = pos.X.ToString() + "|" + pos.Y.ToString();
            if (seen.ContainsKey(key))
            {
                return;
            }
            seen[key] = true;
            list.Add(pos);
        }

        private static int? TryGetRotationCount(ISkillSelectionConfiguration rotation)
        {
            try
            {
                var r1 = rotation as RotationSkillSelection;
                if (r1 != null)
                {
                    return r1.Rotation != null ? (int?)r1.Rotation.Length : null;
                }

                var r2 = rotation as WeightedSkillSelection;
                if (r2 != null)
                {
                    return r2.Rotation != null ? (int?)r2.Rotation.Length : null;
                }

                var r3 = rotation as ConditionalRotationSkillSelection;
                if (r3 != null)
                {
                    return r3.Rotation != null ? (int?)r3.Rotation.Length : null;
                }

                var r4 = rotation as NextSkillNotOnCooldownSelection;
                if (r4 != null)
                {
                    return r4.Rotation != null ? (int?)r4.Rotation.Length : null;
                }
            }
            catch
            {
            }

            return null;
        }

        private static bool TryResolveActivityToRun(
            IGameworldInstance gameworld,
            Entity agent,
            ISkillSelectionConfiguration rotationConfig,
            ulong selectionResult,
            out int? weaponIndex,
            out int? skillIndex,
            out ulong activityId)
        {
            weaponIndex = null;
            skillIndex = null;
            activityId = 0UL;

            if (gameworld == null || gameworld.EntitySystem == null)
            {
                return false;
            }

            // RotationSkillSelection / WeightedSkillSelection / ConditionalRotationSkillSelection are commonly authored
            // as "skill slot indices" (int), not activity ids.
            var isIndexBased =
                rotationConfig is RotationSkillSelection ||
                rotationConfig is WeightedSkillSelection ||
                rotationConfig is ConditionalRotationSkillSelection;

            if (isIndexBased)
            {
                if (selectionResult > (ulong)int.MaxValue)
                {
                    return false;
                }

                return TryResolveActivityFromLoadout(gameworld.EntitySystem, agent, (int)selectionResult, out weaponIndex, out skillIndex, out activityId);
            }

            // NextSkillNotOnCooldownSelection appears to be authored as actual activity ids (ulong).
            if (rotationConfig is NextSkillNotOnCooldownSelection)
            {
                activityId = selectionResult;

                // Best-effort back-mapping to indices for costs/ammo; ok if it fails.
                int wi;
                int si;
                if (TryFindSkillIndicesForActivity(gameworld.EntitySystem, agent, activityId, out wi, out si))
                {
                    weaponIndex = wi;
                    skillIndex = si;
                }

                return activityId != 0UL;
            }

            // Unknown selection type; treat as activity id if it fits.
            activityId = selectionResult;
            return activityId != 0UL;
        }

        private static bool TryResolveActivityFromLoadout(EntitySystem entitySystem, Entity agent, int desiredSkillIndex, out int? weaponIndex, out int? skillIndex, out ulong activityId)
        {
            weaponIndex = null;
            skillIndex = null;
            activityId = 0UL;

            if (entitySystem == null)
            {
                return false;
            }

            ISkillLoadoutComponent loadout;
            if (!entitySystem.TryGetComponent<ISkillLoadoutComponent>(agent, out loadout) || loadout == null)
            {
                return false;
            }

            var selectedWeapon = loadout.SelectedWeapon;
            if (selectedWeapon == null || selectedWeapon.Skills == null)
            {
                return false;
            }

            if (desiredSkillIndex < 0 || desiredSkillIndex >= selectedWeapon.Skills.Length)
            {
                return false;
            }

            var activity = selectedWeapon.Skills[desiredSkillIndex];
            if (activity == null || activity.Id == 0UL)
            {
                return false;
            }

            weaponIndex = loadout.SelectedWeaponIndex;
            skillIndex = desiredSkillIndex;
            activityId = activity.Id;
            return true;
        }

        private static bool TryFindSkillIndicesForActivity(EntitySystem entitySystem, Entity agent, ulong activityId, out int weaponIndex, out int skillIndex)
        {
            weaponIndex = 0;
            skillIndex = 0;

            if (entitySystem == null || activityId == 0UL)
            {
                return false;
            }

            ISkillLoadoutComponent loadout;
            if (!entitySystem.TryGetComponent<ISkillLoadoutComponent>(agent, out loadout) || loadout == null)
            {
                return false;
            }

            var weapons = loadout.Weapons;
            if (weapons == null)
            {
                return false;
            }

            for (var wi = 0; wi < weapons.Length; wi++)
            {
                var w = weapons[wi];
                if (w == null || w.Skills == null)
                {
                    continue;
                }

                for (var si = 0; si < w.Skills.Length; si++)
                {
                    var a = w.Skills[si];
                    if (a != null && a.Id == activityId)
                    {
                        weaponIndex = wi;
                        skillIndex = si;
                        return true;
                    }
                }
            }

            return false;
        }

        private static IntVector2D? TryGetAgentPosition(IGameworldInstance gameworld, Entity agent)
        {
            try
            {
                return gameworld.EntitySystem.GetAgentGridPosition(agent);
            }
            catch
            {
                return null;
            }
        }

        private static IntVector2D? TryGetNearestEnemyPosition(IGameworldInstance gameworld, Entity agent)
        {
            try
            {
                var entitySystem = gameworld != null ? gameworld.EntitySystem : null;
                if (entitySystem == null)
                {
                    return null;
                }

                TeamComponent myTeam;
                if (!entitySystem.TryGetComponent<TeamComponent>(agent, out myTeam) || myTeam == null)
                {
                    return null;
                }

                var myTeamId = myTeam.TeamID;
                IntVector2D myPos;
                try { myPos = entitySystem.GetAgentGridPosition(agent); }
                catch { return null; }

                Entity best = default(Entity);
                var bestDist = int.MaxValue;

                foreach (var other in entitySystem.GetAllEntities())
                {
                    if (other == agent)
                    {
                        continue;
                    }

                    TeamComponent otherTeam;
                    if (!entitySystem.TryGetComponent<TeamComponent>(other, out otherTeam) || otherTeam == null)
                    {
                        continue;
                    }

                    if (otherTeam.TeamID == myTeamId)
                    {
                        continue;
                    }

                    // Skip entities without a valid position.
                    IntVector2D otherPos;
                    try { otherPos = entitySystem.GetAgentGridPosition(other); }
                    catch { continue; }

                    var dist = Math.Abs(otherPos.X - myPos.X) + Math.Abs(otherPos.Y - myPos.Y);
                    if (dist < bestDist)
                    {
                        bestDist = dist;
                        best = other;
                    }
                }

                if (bestDist == int.MaxValue)
                {
                    return null;
                }

                try
                {
                    return entitySystem.GetAgentGridPosition(best);
                }
                catch
                {
                    return null;
                }
            }
            catch
            {
                return null;
            }
        }

        private IntVector2D? TryPickMoveTargetTowardNearestEnemyWithMemory(
            IGameworldInstance gameworld,
            Entity agent,
            out int? debugPreferredEnemyId,
            out int? debugChosenEnemyId,
            out int? debugChosenEnemyTeamId,
            out bool? debugChosenEnemyTeamAi,
            out ulong? debugChosenEnemyControlPlayerId,
            out bool? debugChosenEnemyControlAi,
            out bool? debugChosenEnemyIsPlayersPlayerCharacter,
            out bool? debugChosenEnemyInteractiveObject,
            out string debugEnemyPick,
            out string debugEnemyReason,
            out int? debugEnemyX,
            out int? debugEnemyY,
            out int? debugEnemyCandidateCount,
            out int? debugReachableCellCount,
            out int? debugReducingCellCount,
            out bool? debugAvoidedImmediateBacktrack,
            out int? debugCurrentDistToEnemy)
        {
            debugPreferredEnemyId = null;
            debugChosenEnemyId = null;
            debugChosenEnemyTeamId = null;
            debugChosenEnemyTeamAi = null;
            debugChosenEnemyControlPlayerId = null;
            debugChosenEnemyControlAi = null;
            debugChosenEnemyIsPlayersPlayerCharacter = null;
            debugChosenEnemyInteractiveObject = null;
            debugEnemyPick = null;
            debugEnemyReason = null;
            debugEnemyX = null;
            debugEnemyY = null;
            debugEnemyCandidateCount = null;
            debugReachableCellCount = null;
            debugReducingCellCount = null;
            debugAvoidedImmediateBacktrack = null;
            debugCurrentDistToEnemy = null;

            try
            {
                if (gameworld == null || agent == null)
                {
                    debugEnemyReason = "missing-deps";
                    return null;
                }

                // Prevent unbounded growth in long sessions.
                if (_stickyEnemyByAgentId.Count > 512)
                {
                    _stickyEnemyByAgentId.Clear();
                    _lastMoveFromByAgentId.Clear();
                    _lastMoveToByAgentId.Clear();
                }

                var myPosN = TryGetAgentPosition(gameworld, agent);
                if (!myPosN.HasValue)
                {
                    debugEnemyReason = "no-self-pos";
                    return null;
                }

                var myPos = myPosN.Value;

                int preferredEnemyId;
                var havePreferredEnemy = _stickyEnemyByAgentId.TryGetValue(agent.Id, out preferredEnemyId);
                if (havePreferredEnemy)
                {
                    debugPreferredEnemyId = preferredEnemyId;
                }

                Entity chosenEnemy;
                IntVector2D enemyPos;
                int candidateCount;
                string pick;
                string reason;
                int? chosenEnemyTeamId;
                bool? chosenEnemyTeamAi;
                ulong? chosenEnemyControlPlayerId;
                bool? chosenEnemyControlAi;
                bool? chosenEnemyIsPlayersPlayerCharacter;
                bool? chosenEnemyInteractiveObject;
                if (!TryGetNearestEnemyEntity(
                    gameworld,
                    agent,
                    havePreferredEnemy ? (int?)preferredEnemyId : (int?)null,
                    out chosenEnemy,
                    out enemyPos,
                    out pick,
                    out reason,
                    out candidateCount,
                    out chosenEnemyTeamId,
                    out chosenEnemyTeamAi,
                    out chosenEnemyControlPlayerId,
                    out chosenEnemyControlAi,
                    out chosenEnemyIsPlayersPlayerCharacter,
                    out chosenEnemyInteractiveObject))
                {
                    debugEnemyPick = pick;
                    debugEnemyReason = reason;
                    debugEnemyCandidateCount = candidateCount;
                    return null;
                }

                debugEnemyPick = pick;
                debugEnemyReason = reason;
                debugEnemyCandidateCount = candidateCount;
                if (chosenEnemy != null)
                {
                    debugChosenEnemyId = chosenEnemy.Id;
                }
                debugChosenEnemyTeamId = chosenEnemyTeamId;
                debugChosenEnemyTeamAi = chosenEnemyTeamAi;
                debugChosenEnemyControlPlayerId = chosenEnemyControlPlayerId;
                debugChosenEnemyControlAi = chosenEnemyControlAi;
                debugChosenEnemyIsPlayersPlayerCharacter = chosenEnemyIsPlayersPlayerCharacter;
                debugChosenEnemyInteractiveObject = chosenEnemyInteractiveObject;
                debugEnemyX = enemyPos.X;
                debugEnemyY = enemyPos.Y;

                // Update sticky target once we have a valid one.
                if (chosenEnemy != null)
                {
                    _stickyEnemyByAgentId[agent.Id] = chosenEnemy.Id;
                }

                // C# 3.5 definite-assignment is conservative with struct outs behind short-circuit operators.
                // Initialize to avoid CS0170 (possibly unassigned field X/Y).
                IntVector2D lastFrom = IntVector2D.Zero;
                IntVector2D lastTo = IntVector2D.Zero;
                var haveLastMove = false;
                if (_lastMoveFromByAgentId.TryGetValue(agent.Id, out lastFrom)
                    && _lastMoveToByAgentId.TryGetValue(agent.Id, out lastTo))
                {
                    haveLastMove = true;
                }

                var currentDist = Math.Abs(enemyPos.X - myPos.X) + Math.Abs(enemyPos.Y - myPos.Y);
                debugCurrentDistToEnemy = currentDist;

                var reachableCells = TryGetReachableCells(gameworld, agent);
                var runtimeGrid = gameworld.RuntimeGrid;
                if (reachableCells == null || reachableCells.Count == 0)
                {
                    debugEnemyReason = (debugEnemyReason ?? "") + ";no-reachable";
                    return null;
                }

                debugReachableCellCount = reachableCells.Count;

                // Prefer a cell that strictly reduces manhattan distance to the chosen enemy.
                // If we can't reduce distance, allow a small "detour" step (equal or +1 manhattan)
                // to route around obstacles, but never allow large increases.
                var bestReducingDist = int.MaxValue;
                IntVector2D bestReducing = IntVector2D.Zero;
                var haveReducing = false;

                var bestReducingBacktrackDist = int.MaxValue;
                IntVector2D bestReducingBacktrack = IntVector2D.Zero;
                var haveReducingBacktrack = false;

                var bestDetourDist = int.MaxValue;
                IntVector2D bestDetour = IntVector2D.Zero;
                var haveDetour = false;
                var detourCount = 0;

                var avoidedBacktrack = false;
                var reducingCount = 0;
                foreach (var cell in reachableCells)
                {
                    if (cell.X == myPos.X && cell.Y == myPos.Y)
                    {
                        continue;
                    }

                    if (runtimeGrid != null)
                    {
                        Entity occ;
                        try { occ = runtimeGrid.GetEntityFromPosition(cell); }
                        catch { occ = null; }

                        if (occ != null && occ != agent)
                        {
                            continue;
                        }

                        Entity staticOcc;
                        try { staticOcc = runtimeGrid.GetStaticEntityFromPosition(cell); }
                        catch { staticOcc = null; }

                        if (staticOcc != null)
                        {
                            continue;
                        }
                    }

                    var dist = Math.Abs(enemyPos.X - cell.X) + Math.Abs(enemyPos.Y - cell.Y);
                    if (dist < currentDist)
                    {
                        reducingCount++;
                    }
                    else
                    {
                        // Detour candidate: allow equal distance or +1 only.
                        // This helps when a wall forces a lateral move before distance can reduce.
                        if (dist <= currentDist + 1)
                        {
                            detourCount++;
                        }
                        else
                        {
                            continue;
                        }
                    }

                    var isImmediateBacktrack = haveLastMove
                        && myPos.X == lastTo.X && myPos.Y == lastTo.Y
                        && cell.X == lastFrom.X && cell.Y == lastFrom.Y;

                    if (isImmediateBacktrack)
                    {
                        avoidedBacktrack = true;
                        if (dist < currentDist)
                        {
                            if (!haveReducingBacktrack
                                || dist < bestReducingBacktrackDist
                                || (dist == bestReducingBacktrackDist && (cell.X < bestReducingBacktrack.X || (cell.X == bestReducingBacktrack.X && cell.Y < bestReducingBacktrack.Y))))
                            {
                                bestReducingBacktrackDist = dist;
                                bestReducingBacktrack = cell;
                                haveReducingBacktrack = true;
                            }
                        }
                        continue;
                    }

                    if (dist < currentDist)
                    {
                        if (!haveReducing
                            || dist < bestReducingDist
                            || (dist == bestReducingDist && (cell.X < bestReducing.X || (cell.X == bestReducing.X && cell.Y < bestReducing.Y))))
                        {
                            bestReducingDist = dist;
                            bestReducing = cell;
                            haveReducing = true;
                        }
                    }
                    else
                    {
                        // Detour step (equal or +1), only used when no reducing tile exists.
                        if (!haveDetour
                            || dist < bestDetourDist
                            || (dist == bestDetourDist && (cell.X < bestDetour.X || (cell.X == bestDetour.X && cell.Y < bestDetour.Y))))
                        {
                            bestDetourDist = dist;
                            bestDetour = cell;
                            haveDetour = true;
                        }
                    }
                }

                debugReducingCellCount = reducingCount;
                if (avoidedBacktrack)
                {
                    debugAvoidedImmediateBacktrack = true;
                }

                IntVector2D chosen;
                if (haveReducing)
                {
                    chosen = bestReducing;
                }
                else if (haveReducingBacktrack)
                {
                    // If backtracking is the only way to reduce distance, allow it.
                    chosen = bestReducingBacktrack;
                }
                else if (haveDetour && currentDist > 1)
                {
                    // Detour only when we're not already adjacent.
                    chosen = bestDetour;

                    // Make the log reason explicit.
                    if (debugEnemyReason == null)
                    {
                        debugEnemyReason = "detour";
                    }
                    else
                    {
                        debugEnemyReason = debugEnemyReason + ";detour";
                    }
                }
                else
                {
                    // No progress available.
                    return null;
                }

                // Remember this move so we can avoid immediate reversals on the next Decide call.
                _lastMoveFromByAgentId[agent.Id] = myPos;
                _lastMoveToByAgentId[agent.Id] = chosen;

                return chosen;
            }
            catch
            {
                return null;
            }
        }

        private static bool TryGetNearestEnemyEntity(
            IGameworldInstance gameworld,
            Entity agent,
            int? preferredEnemyId,
            out Entity enemy,
            out IntVector2D enemyPos,
            out string debugPick,
            out string debugReason,
            out int candidateCount,
            out int? debugChosenEnemyTeamId,
            out bool? debugChosenEnemyTeamAi,
            out ulong? debugChosenEnemyControlPlayerId,
            out bool? debugChosenEnemyControlAi,
            out bool? debugChosenEnemyIsPlayersPlayerCharacter,
            out bool? debugChosenEnemyInteractiveObject)
        {
            enemy = null;
            enemyPos = IntVector2D.Zero;
            debugPick = null;
            debugReason = null;
            candidateCount = 0;
            debugChosenEnemyTeamId = null;
            debugChosenEnemyTeamAi = null;
            debugChosenEnemyControlPlayerId = null;
            debugChosenEnemyControlAi = null;
            debugChosenEnemyIsPlayersPlayerCharacter = null;
            debugChosenEnemyInteractiveObject = null;

            try
            {
                var entitySystem = gameworld != null ? gameworld.EntitySystem : null;
                if (entitySystem == null)
                {
                    debugReason = "no-entity-system";
                    return false;
                }

                TeamComponent myTeam;
                if (!entitySystem.TryGetComponent<TeamComponent>(agent, out myTeam) || myTeam == null)
                {
                    debugReason = "no-team";
                    return false;
                }

                var myTeamId = myTeam.TeamID;

                IntVector2D myPos;
                try { myPos = entitySystem.GetAgentGridPosition(agent); }
                catch
                {
                    if (debugReason == null)
                    {
                        debugReason = "no-self-pos";
                    }
                    return false;
                }

                // Build a candidate list. We intentionally match the roster filter: must be on a team,
                // must have GameplayPropertiesComponent, and must NOT be an interactive object.
                //
                // Additionally, prefer player-controlled targets (controlAi==false) and then non-AI teams.
                var bestTier = int.MaxValue;
                Entity best = default(Entity);
                IntVector2D bestPos = IntVector2D.Zero;
                var bestDist = int.MaxValue;

                Entity preferredCandidate = null;
                IntVector2D preferredPos = IntVector2D.Zero;
                int preferredTier = int.MaxValue;
                var havePreferredCandidate = false;

                foreach (var other in entitySystem.GetAllEntities())
                {
                    if (other == agent)
                    {
                        continue;
                    }

                    TeamComponent otherTeam;
                    if (!entitySystem.TryGetComponent<TeamComponent>(other, out otherTeam) || otherTeam == null)
                    {
                        continue;
                    }

                    if (otherTeam.TeamID == myTeamId)
                    {
                        continue;
                    }

                    GameplayPropertiesComponent gp;
                    if (!entitySystem.TryGetComponent<GameplayPropertiesComponent>(other, out gp) || gp == null)
                    {
                        continue;
                    }

                    if (gp.InteractiveObject)
                    {
                        continue;
                    }

                    IntVector2D otherPos;
                    try { otherPos = entitySystem.GetAgentGridPosition(other); }
                    catch { continue; }

                    candidateCount++;

                    ControlComponent control;
                    entitySystem.TryGetComponent<ControlComponent>(other, out control);

                    // Tier 0: explicitly player-controlled (not AI controlled).
                    // Tier 1: belongs to a non-AI team.
                    // Tier 2: everything else.
                    var tier = 2;
                    if (control != null && !control.IsAIControlled)
                    {
                        tier = 0;
                    }
                    else
                    {
                        bool teamAi = false;
                        try { teamAi = otherTeam.IsAIControlled(); }
                        catch { teamAi = false; }

                        if (!teamAi)
                        {
                            tier = 1;
                        }
                    }

                    if (tier < bestTier)
                    {
                        bestTier = tier;
                        bestDist = int.MaxValue;
                        best = default(Entity);
                    }

                    if (tier != bestTier)
                    {
                        // Only select from the best available tier.
                        // (We still count all candidates for debugging.)
                        if (preferredEnemyId.HasValue && other.Id == preferredEnemyId.Value)
                        {
                            // Remember preferred candidate info for later.
                            preferredCandidate = other;
                            preferredPos = otherPos;
                            preferredTier = tier;
                            havePreferredCandidate = true;
                        }
                        continue;
                    }

                    var dist = Math.Abs(otherPos.X - myPos.X) + Math.Abs(otherPos.Y - myPos.Y);
                    if (dist < bestDist || (dist == bestDist && other.Id < best.Id))
                    {
                        bestDist = dist;
                        best = other;
                        bestPos = otherPos;
                    }

                    if (preferredEnemyId.HasValue && other.Id == preferredEnemyId.Value)
                    {
                        preferredCandidate = other;
                        preferredPos = otherPos;
                        preferredTier = tier;
                        havePreferredCandidate = true;
                    }
                }

                if (bestTier == int.MaxValue || bestDist == int.MaxValue)
                {
                    if (debugReason == null)
                    {
                        debugReason = "no-enemies";
                    }
                    return false;
                }

                // Sticky preference: keep preferred if it's still present AND in the best available tier.
                if (preferredEnemyId.HasValue && havePreferredCandidate && preferredCandidate != null && preferredTier == bestTier)
                {
                    enemy = preferredCandidate;
                    enemyPos = preferredPos;
                    debugPick = "sticky";
                    debugReason = "preferred-ok";
                }
                else
                {
                    enemy = best;
                    enemyPos = bestPos;
                    if (debugPick == null)
                    {
                        debugPick = bestTier == 0 ? "nearest-player" : (bestTier == 1 ? "nearest-team" : "nearest");
                    }
                    if (debugReason == null)
                    {
                        debugReason = bestTier == 0 ? "fallback-nearest-player" : (bestTier == 1 ? "fallback-nearest-team" : "fallback-nearest");
                    }
                }

                // Populate chosen-enemy debug info.
                if (enemy != null)
                {
                    TeamComponent chosenTeam;
                    if (entitySystem.TryGetComponent<TeamComponent>(enemy, out chosenTeam) && chosenTeam != null)
                    {
                        debugChosenEnemyTeamId = (int)chosenTeam.TeamID;
                        try { debugChosenEnemyTeamAi = chosenTeam.IsAIControlled(); }
                        catch { debugChosenEnemyTeamAi = null; }
                    }

                    ControlComponent chosenControl;
                    if (entitySystem.TryGetComponent<ControlComponent>(enemy, out chosenControl) && chosenControl != null)
                    {
                        debugChosenEnemyControlPlayerId = chosenControl.PlayerId;
                        debugChosenEnemyControlAi = chosenControl.IsAIControlled;
                    }

                    GameplayPropertiesComponent chosenGp;
                    if (entitySystem.TryGetComponent<GameplayPropertiesComponent>(enemy, out chosenGp) && chosenGp != null)
                    {
                        debugChosenEnemyIsPlayersPlayerCharacter = chosenGp.IsPlayersPlayerCharacter;
                        debugChosenEnemyInteractiveObject = chosenGp.InteractiveObject;
                    }
                }

                return true;
            }
            catch
            {
                enemy = null;
                enemyPos = IntVector2D.Zero;
                if (debugReason == null)
                {
                    debugReason = "exception";
                }
                return false;
            }
        }

        private static HashSet<IntVector2D> TryGetReachableCells(IGameworldInstance gameworld, Entity agent)
        {
            try
            {
                if (gameworld == null || gameworld.ReachableRangesCalculator == null)
                {
                    return null;
                }

                var ranges = gameworld.ReachableRangesCalculator.GetReachableRanges(agent);
                if (ranges == null)
                {
                    return null;
                }

                var effective = ranges.EffectiveMovementRange;
                if (effective == null)
                {
                    return null;
                }

                return effective.ReachableCells;
            }
            catch
            {
                return null;
            }
        }

        private static IntVector2D? TryPickMoveTargetToEnableActivity(IGameworldInstance gameworld, Entity agent, int weaponIndex, int skillIndex, ulong activityId)
        {
            try
            {
                if (gameworld == null || agent == null || activityId == 0UL)
                {
                    return null;
                }

                if (gameworld.ActivitySystem == null)
                {
                    return null;
                }

                var dryRunner = gameworld.ActivitySystem as IActivitySystemDryRunner;
                if (dryRunner == null)
                {
                    return null;
                }

                var myPosN = TryGetAgentPosition(gameworld, agent);
                if (!myPosN.HasValue)
                {
                    return null;
                }
                var myPos = myPosN.Value;

                var reachableCells = TryGetReachableCells(gameworld, agent);
                if (reachableCells == null || reachableCells.Count == 0)
                {
                    return null;
                }

                var runtimeGrid = gameworld.RuntimeGrid;

                var bestMoveDist = int.MaxValue;
                IntVector2D best = IntVector2D.Zero;
                var haveBest = false;

                foreach (var cell in reachableCells)
                {
                    if (cell.X == myPos.X && cell.Y == myPos.Y)
                    {
                        continue;
                    }

                    if (runtimeGrid != null)
                    {
                        Entity occ;
                        try { occ = runtimeGrid.GetEntityFromPosition(cell); }
                        catch { occ = null; }

                        if (occ != null && occ != agent)
                        {
                            continue;
                        }

                        Entity staticOcc;
                        try { staticOcc = runtimeGrid.GetStaticEntityFromPosition(cell); }
                        catch { staticOcc = null; }

                        if (staticOcc != null)
                        {
                            continue;
                        }
                    }

                    ActivityEvaluationResult eval;
                    try
                    {
                        eval = dryRunner.DryRunActivity(weaponIndex, skillIndex, activityId, agent, cell);
                    }
                    catch
                    {
                        eval = null;
                    }

                    if (eval == null || !eval.SkillWasSuccessful || eval.TargetWorkspaces == null || eval.TargetWorkspaces.AvailableTargets <= 0)
                    {
                        continue;
                    }

                    var moveDist = Math.Abs(cell.X - myPos.X) + Math.Abs(cell.Y - myPos.Y);
                    if (!haveBest
                        || moveDist < bestMoveDist
                        || (moveDist == bestMoveDist && (cell.X < best.X || (cell.X == best.X && cell.Y < best.Y))))
                    {
                        bestMoveDist = moveDist;
                        best = cell;
                        haveBest = true;
                    }
                }

                if (haveBest)
                {
                    return best;
                }

                return null;
            }
            catch
            {
                return null;
            }
        }

        private static IntVector2D? TryPickMoveTargetTowardNearestEnemy(IGameworldInstance gameworld, Entity agent)
        {
            try
            {
                if (gameworld == null || agent == null)
                {
                    return null;
                }

                var myPosN = TryGetAgentPosition(gameworld, agent);
                if (!myPosN.HasValue)
                {
                    return null;
                }

                var enemyPosN = TryGetNearestEnemyPosition(gameworld, agent);
                if (!enemyPosN.HasValue)
                {
                    return null;
                }

                var myPos = myPosN.Value;
                var enemyPos = enemyPosN.Value;
                var currentDist = Math.Abs(enemyPos.X - myPos.X) + Math.Abs(enemyPos.Y - myPos.Y);

                var reachableCells = TryGetReachableCells(gameworld, agent);
                var runtimeGrid = gameworld.RuntimeGrid;

                // Prefer a cell that strictly reduces manhattan distance to the nearest enemy.
                // If we can't find one (e.g. boxed in), fall back to the closest reachable cell.
                if (reachableCells != null && reachableCells.Count > 0)
                {
                    var bestReducingDist = int.MaxValue;
                    IntVector2D bestReducing = IntVector2D.Zero;
                    var haveReducing = false;

                    var bestAnyDist = int.MaxValue;
                    IntVector2D bestAny = IntVector2D.Zero;
                    var haveAny = false;

                    foreach (var cell in reachableCells)
                    {
                        if (cell.X == myPos.X && cell.Y == myPos.Y)
                        {
                            continue;
                        }

                        if (runtimeGrid != null)
                        {
                            // Avoid moving onto an occupied or static-blocked cell.
                            Entity occ;
                            try { occ = runtimeGrid.GetEntityFromPosition(cell); }
                            catch { occ = null; }

                            if (occ != null && occ != agent)
                            {
                                continue;
                            }

                            Entity staticOcc;
                            try { staticOcc = runtimeGrid.GetStaticEntityFromPosition(cell); }
                            catch { staticOcc = null; }

                            if (staticOcc != null)
                            {
                                continue;
                            }
                        }

                        var dist = Math.Abs(enemyPos.X - cell.X) + Math.Abs(enemyPos.Y - cell.Y);
                        if (dist < bestAnyDist || (dist == bestAnyDist && haveAny && (cell.X < bestAny.X || (cell.X == bestAny.X && cell.Y < bestAny.Y))))
                        {
                            bestAnyDist = dist;
                            bestAny = cell;
                            haveAny = true;
                        }

                        if (dist < currentDist && (dist < bestReducingDist || (dist == bestReducingDist && haveReducing && (cell.X < bestReducing.X || (cell.X == bestReducing.X && cell.Y < bestReducing.Y)))))
                        {
                            bestReducingDist = dist;
                            bestReducing = cell;
                            haveReducing = true;
                        }
                    }

                    if (haveReducing)
                    {
                        return bestReducing;
                    }

                    if (haveAny)
                    {
                        return bestAny;
                    }
                }

                // If we couldn't get reachable cells, try a basic "closest free" around the enemy.
                if (runtimeGrid != null)
                {
                    try
                    {
                        var closestFree = runtimeGrid.GetClosestFreePosition(enemyPos, false);
                        if (closestFree.X != myPos.X || closestFree.Y != myPos.Y)
                        {
                            return closestFree;
                        }
                    }
                    catch
                    {
                    }
                }

                // Final fallback: attempt to move toward the enemy position (may be blocked, but better than nothing).
                if (enemyPos.X != myPos.X || enemyPos.Y != myPos.Y)
                {
                    return enemyPos;
                }

                return null;
            }
            catch
            {
                return null;
            }
        }
    }
}
