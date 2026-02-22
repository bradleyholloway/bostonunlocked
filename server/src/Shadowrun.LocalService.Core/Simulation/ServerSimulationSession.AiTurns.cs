using System.Collections.Generic;
using System.Linq;
using Cliffhanger.SRO.ServerClientCommons.Gameworld;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.CommandProcessing;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.Commands;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.Communication;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.Locomotion;
using Cliffhanger.SRO.ServerClientCommons.Gameworld.RandomNumbers;
using SRO.Core.Compatibility.Math;
using SRO.Core.Compatibility.Utilities;
using Shadowrun.LocalService.Core.AILogic;

namespace Shadowrun.LocalService.Core.Simulation
{
    public sealed partial class ServerSimulationSession
    {
        public IList<AiTurnAction> SkipAiTurnsIfNeeded()
        {
            var actions = new List<AiTurnAction>();

            // Safety: avoid infinite loops if the sim gets into a bad state.
            for (var i = 0; i < 64; i++)
            {
                if (_simulation.IsMissionStopped)
                {
                    break;
                }

                var team = _turnObserver.CurrentTeam;
                if (team == null)
                {
                    _logger.Log(new
                    {
                        ts = RequestLogger.UtcNowIso(),
                        type = "sim",
                        peer = _peer,
                        action = "skip-ai",
                        status = "no-current-team",
                    });
                    break;
                }

                if (!team.AIControlled)
                {
                    _logger.Log(new
                    {
                        ts = RequestLogger.UtcNowIso(),
                        type = "sim",
                        peer = _peer,
                        action = "skip-ai",
                        status = "current-team-not-ai",
                        teamId = team.ID,
                    });
                    break;
                }

                AiTurnAction action;
                if (!TryExecuteAiTurnStep(team, out action))
                {
                    break;
                }

                actions.Add(action);
            }

            if (actions.Count > 0)
            {
                _logger.Log(new
                {
                    ts = RequestLogger.UtcNowIso(),
                    type = "sim",
                    peer = _peer,
                    action = "skip-ai",
                    count = actions.Count,
                    agents = actions.Select(a => a.AgentId).ToArray(),
                });
            }

            return actions;
        }

        private bool TryExecuteAiTurnStep(Team team, out AiTurnAction action)
        {
            action = null;

            // Choose a valid activatable member; never guess IDs.
            if (_turnObserver.CurrentActivatableMembers == null || _turnObserver.CurrentActivatableMembers.Length == 0)
            {
                // No-one can act, simulation should advance on its own. If it doesn't, break.
                _logger.Log(new
                {
                    ts = RequestLogger.UtcNowIso(),
                    type = "sim",
                    peer = _peer,
                    action = "skip-ai",
                    status = "no-activatable-members",
                    teamId = team.ID,
                });
                return false;
            }

            var agent = _turnObserver.CurrentActivatableMembers[0];

            var targetPos = TryGetAgentGridPositionOrDefault(agent);
            var commandName = "AI.EndTeamTurn";

            ulong desiredSkill = 0UL;
            int desiredWeaponIndex = 0;
            int desiredSkillIndex = 0;
            string decisionNote = null;
            string debugStage = null;
            string debugRotationType = null;
            int? debugRotationCount = null;
            ulong debugRawSelection = 0UL;
            ulong debugResolvedActivityId = 0UL;
            bool? debugHasAiConfig = null;
            bool? debugHasLoadout = null;
            int? debugSelectedWeaponIndex = null;
            int? debugSelectedWeaponSkillCount = null;
            int? debugPreferredEnemyId = null;
            int? debugChosenEnemyId = null;
            int? debugChosenEnemyTeamId = null;
            bool? debugChosenEnemyTeamAi = null;
            ulong? debugChosenEnemyControlPlayerId = null;
            bool? debugChosenEnemyControlAi = null;
            bool? debugChosenEnemyIsPlayersPlayerCharacter = null;
            bool? debugChosenEnemyInteractiveObject = null;
            string debugEnemyPick = null;
            string debugEnemyReason = null;
            int? debugEnemyX = null;
            int? debugEnemyY = null;
            int? debugEnemyCandidateCount = null;
            int? debugReachableCellCount = null;
            int? debugReducingCellCount = null;
            bool? debugAvoidedImmediateBacktrack = null;
            int? debugCurrentDistToEnemy = null;
            if (_enableAiLogic && _aiDecisionEngine != null)
            {
                try
                {
                    var decision = _aiDecisionEngine.Decide(agent, _gameworld);
                    if (decision != null && decision.HasAction && decision.IsMove)
                    {
                        commandName = "AI.Move";
                        decisionNote = "move";
                        targetPos = decision.MoveTargetPosition;

                        debugStage = decision.DebugStage;
                        debugRotationType = decision.DebugRotationType;
                        debugRotationCount = decision.DebugRotationCount;
                        debugRawSelection = decision.DebugRawSelection;
                        debugResolvedActivityId = decision.DebugResolvedActivityId;
                        debugHasAiConfig = decision.DebugHasAiConfig;
                        debugHasLoadout = decision.DebugHasLoadout;
                        debugSelectedWeaponIndex = decision.DebugSelectedWeaponIndex;
                        debugSelectedWeaponSkillCount = decision.DebugSelectedWeaponSkillCount;

                        debugPreferredEnemyId = decision.DebugPreferredEnemyId;
                        debugChosenEnemyId = decision.DebugChosenEnemyId;
                        debugChosenEnemyTeamId = decision.DebugChosenEnemyTeamId;
                        debugChosenEnemyTeamAi = decision.DebugChosenEnemyTeamAi;
                        debugChosenEnemyControlPlayerId = decision.DebugChosenEnemyControlPlayerId;
                        debugChosenEnemyControlAi = decision.DebugChosenEnemyControlAi;
                        debugChosenEnemyIsPlayersPlayerCharacter = decision.DebugChosenEnemyIsPlayersPlayerCharacter;
                        debugChosenEnemyInteractiveObject = decision.DebugChosenEnemyInteractiveObject;
                        debugEnemyPick = decision.DebugEnemyPick;
                        debugEnemyReason = decision.DebugEnemyReason;
                        debugEnemyX = decision.DebugEnemyX;
                        debugEnemyY = decision.DebugEnemyY;
                        debugEnemyCandidateCount = decision.DebugEnemyCandidateCount;
                        debugReachableCellCount = decision.DebugReachableCellCount;
                        debugReducingCellCount = decision.DebugReducingCellCount;
                        debugAvoidedImmediateBacktrack = decision.DebugAvoidedImmediateBacktrack;
                        debugCurrentDistToEnemy = decision.DebugCurrentDistToEnemy;
                    }
                    else if (decision != null && decision.HasAction && decision.SkillId != 0UL)
                    {
                        desiredSkill = decision.SkillId;
                        commandName = "AI.Decision";
                        decisionNote = "ok";

                        if (decision.WeaponIndex.HasValue)
                        {
                            desiredWeaponIndex = decision.WeaponIndex.Value;
                        }
                        if (decision.SkillIndex.HasValue)
                        {
                            desiredSkillIndex = decision.SkillIndex.Value;
                        }

                        debugStage = decision.DebugStage;
                        debugRotationType = decision.DebugRotationType;
                        debugRotationCount = decision.DebugRotationCount;
                        debugRawSelection = decision.DebugRawSelection;
                        debugResolvedActivityId = decision.DebugResolvedActivityId;
                        debugHasAiConfig = decision.DebugHasAiConfig;
                        debugHasLoadout = decision.DebugHasLoadout;
                        debugSelectedWeaponIndex = decision.DebugSelectedWeaponIndex;
                        debugSelectedWeaponSkillCount = decision.DebugSelectedWeaponSkillCount;

                        debugPreferredEnemyId = decision.DebugPreferredEnemyId;
                        debugChosenEnemyId = decision.DebugChosenEnemyId;
                        debugChosenEnemyTeamId = decision.DebugChosenEnemyTeamId;
                        debugChosenEnemyTeamAi = decision.DebugChosenEnemyTeamAi;
                        debugChosenEnemyControlPlayerId = decision.DebugChosenEnemyControlPlayerId;
                        debugChosenEnemyControlAi = decision.DebugChosenEnemyControlAi;
                        debugChosenEnemyIsPlayersPlayerCharacter = decision.DebugChosenEnemyIsPlayersPlayerCharacter;
                        debugChosenEnemyInteractiveObject = decision.DebugChosenEnemyInteractiveObject;
                        debugEnemyPick = decision.DebugEnemyPick;
                        debugEnemyReason = decision.DebugEnemyReason;
                        debugEnemyX = decision.DebugEnemyX;
                        debugEnemyY = decision.DebugEnemyY;
                        debugEnemyCandidateCount = decision.DebugEnemyCandidateCount;
                        debugReachableCellCount = decision.DebugReachableCellCount;
                        debugReducingCellCount = decision.DebugReducingCellCount;
                        debugAvoidedImmediateBacktrack = decision.DebugAvoidedImmediateBacktrack;
                        debugCurrentDistToEnemy = decision.DebugCurrentDistToEnemy;

                        if (decision.TargetEntity != null)
                        {
                            targetPos = TryGetAgentGridPositionOrDefault(decision.TargetEntity);
                        }
                        else
                        {
                            targetPos = decision.TargetPosition;
                        }
                    }
                    else
                    {
                        decisionNote = decision == null ? "null-decision" : "no-action";

                        if (decision != null)
                        {
                            debugStage = decision.DebugStage;
                            debugRotationType = decision.DebugRotationType;
                            debugRotationCount = decision.DebugRotationCount;
                            debugRawSelection = decision.DebugRawSelection;
                            debugResolvedActivityId = decision.DebugResolvedActivityId;
                            debugHasAiConfig = decision.DebugHasAiConfig;
                            debugHasLoadout = decision.DebugHasLoadout;
                            debugSelectedWeaponIndex = decision.DebugSelectedWeaponIndex;
                            debugSelectedWeaponSkillCount = decision.DebugSelectedWeaponSkillCount;

                            debugPreferredEnemyId = decision.DebugPreferredEnemyId;
                            debugChosenEnemyId = decision.DebugChosenEnemyId;
                            debugChosenEnemyTeamId = decision.DebugChosenEnemyTeamId;
                            debugChosenEnemyTeamAi = decision.DebugChosenEnemyTeamAi;
                            debugChosenEnemyControlPlayerId = decision.DebugChosenEnemyControlPlayerId;
                            debugChosenEnemyControlAi = decision.DebugChosenEnemyControlAi;
                            debugChosenEnemyIsPlayersPlayerCharacter = decision.DebugChosenEnemyIsPlayersPlayerCharacter;
                            debugChosenEnemyInteractiveObject = decision.DebugChosenEnemyInteractiveObject;
                            debugEnemyPick = decision.DebugEnemyPick;
                            debugEnemyReason = decision.DebugEnemyReason;
                            debugEnemyX = decision.DebugEnemyX;
                            debugEnemyY = decision.DebugEnemyY;
                            debugEnemyCandidateCount = decision.DebugEnemyCandidateCount;
                            debugReachableCellCount = decision.DebugReachableCellCount;
                            debugReducingCellCount = decision.DebugReducingCellCount;
                            debugAvoidedImmediateBacktrack = decision.DebugAvoidedImmediateBacktrack;
                            debugCurrentDistToEnemy = decision.DebugCurrentDistToEnemy;
                        }
                    }
                }
                catch
                {
                    desiredSkill = 0UL;
                    commandName = "AI.EndTeamTurn";
                    decisionNote = "exception";
                    debugStage = "exception";
                }
            }

            if (_enableAiLogic)
            {
                try
                {
                    _logger.LogAi(new
                    {
                        ts = RequestLogger.UtcNowIso(),
                        type = "ai",
                        peer = _peer,
                        teamId = team != null ? (int?)team.ID : null,
                        agentId = agent != null ? (int?)agent.Id : null,
                        decisionSkillId = desiredSkill,
                        decisionWeaponIndex = desiredWeaponIndex,
                        decisionSkillIndex = desiredSkillIndex,
                        decisionNote = decisionNote,
                        debugStage = debugStage,
                        debugRotationType = debugRotationType,
                        debugRotationCount = debugRotationCount,
                        debugRawSelection = debugRawSelection,
                        debugResolvedActivityId = debugResolvedActivityId,
                        debugHasAiConfig = debugHasAiConfig,
                        debugHasLoadout = debugHasLoadout,
                        debugSelectedWeaponIndex = debugSelectedWeaponIndex,
                        debugSelectedWeaponSkillCount = debugSelectedWeaponSkillCount,
                        debugPreferredEnemyId = debugPreferredEnemyId,
                        debugChosenEnemyId = debugChosenEnemyId,
                        debugChosenEnemyTeamId = debugChosenEnemyTeamId,
                        debugChosenEnemyTeamAi = debugChosenEnemyTeamAi,
                        debugChosenEnemyControlPlayerId = debugChosenEnemyControlPlayerId,
                        debugChosenEnemyControlAi = debugChosenEnemyControlAi,
                        debugChosenEnemyIsPlayersPlayerCharacter = debugChosenEnemyIsPlayersPlayerCharacter,
                        debugChosenEnemyInteractiveObject = debugChosenEnemyInteractiveObject,
                        debugEnemyPick = debugEnemyPick,
                        debugEnemyReason = debugEnemyReason,
                        debugEnemyX = debugEnemyX,
                        debugEnemyY = debugEnemyY,
                        debugEnemyCandidateCount = debugEnemyCandidateCount,
                        debugReachableCellCount = debugReachableCellCount,
                        debugReducingCellCount = debugReducingCellCount,
                        debugAvoidedImmediateBacktrack = debugAvoidedImmediateBacktrack,
                        debugCurrentDistToEnemy = debugCurrentDistToEnemy,
                        command = commandName,
                        targetX = targetPos.X,
                        targetY = targetPos.Y,
                    });
                }
                catch
                {
                }
            }

            var skillToUse = EndTeamTurnSkillId;
            int? skillToLog = EndTeamTurnSkillId;
            if (desiredSkill > 0UL && desiredSkill <= (ulong)int.MaxValue)
            {
                skillToUse = (int)desiredSkill;
                skillToLog = (int)desiredSkill;
            }

            var seeds = _random.CreateSeedPackage();

            ICommand cmd;
            try
            {
                if (commandName == "AI.Move")
                {
                    cmd = new FollowPathCommand(agent.Id, targetPos, _gameworld);
                }
                else
                {
                    cmd = new ActivatePositionTargetedActiveSkillCommand(
                        desiredWeaponIndex,
                        desiredSkillIndex,
                        skillToUse,
                        agent.Id,
                        targetPos,
                        _gameworld,
                        _random,
                        seeds);
                }
            }
            catch
            {
                // Fall back to end-turn if decision command construction fails.
                commandName = "AI.EndTeamTurn";
                skillToUse = EndTeamTurnSkillId;
                skillToLog = EndTeamTurnSkillId;
                cmd = new ActivatePositionTargetedActiveSkillCommand(
                    0,
                    0,
                    skillToUse,
                    agent.Id,
                    TryGetAgentGridPositionOrDefault(agent),
                    _gameworld,
                    _random,
                    seeds);
            }

            bool madeProgress;
            if (commandName == "AI.Move")
            {
                madeProgress = ExecuteCommand(cmd, commandName, null, null, null, targetPos.X, targetPos.Y);
            }
            else
            {
                madeProgress = ExecuteCommand(cmd, commandName, desiredWeaponIndex, desiredSkillIndex, skillToLog, targetPos.X, targetPos.Y);
            }

            // If the AI command was effectively a no-op (common when activity conditions fail), don't spin.
            // Instead, end the unit/team turn and move on.
            if (!madeProgress && commandName != "AI.EndTeamTurn")
            {
                var fallbackSeeds = _random.CreateSeedPackage();
                var fallbackPos = TryGetAgentGridPositionOrDefault(agent);
                var fallbackCmd = new ActivatePositionTargetedActiveSkillCommand(
                    0,
                    0,
                    EndTeamTurnSkillId,
                    agent.Id,
                    fallbackPos,
                    _gameworld,
                    _random,
                    fallbackSeeds);

                var fallbackProgress = ExecuteCommand(fallbackCmd, "AI.EndTeamTurn", 0, 0, EndTeamTurnSkillId, fallbackPos.X, fallbackPos.Y);
                if (!fallbackProgress)
                {
                    return false;
                }

                action = new AiTurnAction(
                    AiTurnActionKind.ActivateActiveSkill,
                    agent.Id,
                    0,
                    0,
                    0,
                    0,
                    EndTeamTurnSkillId,
                    fallbackSeeds);
                return true;
            }

            if (commandName == "AI.Move")
            {
                action = new AiTurnAction(
                    AiTurnActionKind.FollowPath,
                    agent.Id,
                    targetPos.X,
                    targetPos.Y,
                    0,
                    0,
                    0,
                    new SeedPackage(0u, 0u, 0u, 0u));
                return true;
            }

            action = new AiTurnAction(
                AiTurnActionKind.ActivateActiveSkill,
                agent.Id,
                targetPos.X,
                targetPos.Y,
                desiredWeaponIndex,
                desiredSkillIndex,
                skillToUse,
                seeds);
            return true;
        }
    }
}
