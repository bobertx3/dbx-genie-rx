/**
 * Feedback page showing summary of labeling session results.
 * Includes auto-label summary stats and per-question details.
 */

import { useMemo } from "react"
import { ArrowLeft, Sparkles, Check, X, Bot, User } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { getSelectedBenchmarkQuestions } from "@/lib/benchmarkUtils"
import type { AutoLabelResult } from "@/types"

interface FeedbackPageProps {
  spaceData: Record<string, unknown>
  selectedQuestions: string[]
  correctAnswers: Record<string, boolean | null>
  feedbackTexts: Record<string, string>
  autoLabelResults: Record<string, AutoLabelResult>
  userOverrides: Record<string, boolean>
  overrideReasons: Record<string, string>
  onBack: () => void
  onBeginOptimization: () => void
}

export function FeedbackPage({
  spaceData,
  selectedQuestions,
  correctAnswers,
  feedbackTexts,
  autoLabelResults,
  userOverrides,
  overrideReasons,
  onBack,
  onBeginOptimization,
}: FeedbackPageProps) {
  // Get the selected questions in order
  const questions = useMemo(
    () => getSelectedBenchmarkQuestions(spaceData, selectedQuestions),
    [spaceData, selectedQuestions]
  )

  const correctCount = questions.filter(q => correctAnswers[q.id] === true).length
  const incorrectCount = questions.filter(q => correctAnswers[q.id] === false).length

  // Auto-label summary stats
  const autoLabeledCorrect = questions.filter(q => autoLabelResults[q.id]?.auto_label === "correct").length
  const autoLabeledIncorrect = questions.filter(q => autoLabelResults[q.id]?.auto_label === "incorrect").length
  const inconclusiveCount = questions.filter(q => autoLabelResults[q.id]?.auto_label === "inconclusive").length
  const overrideCount = questions.filter(q => userOverrides[q.id] === true).length
  const hasAutoLabels = Object.keys(autoLabelResults).length > 0

  return (
    <div className="space-y-6 animate-slide-up">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-primary">
            Labeling Feedback Summary
          </h1>
          <p className="text-muted">
            {questions.length} questions reviewed
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" onClick={onBack}>
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Labeling
          </Button>
          <Button
            onClick={onBeginOptimization}
            className="bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-600 hover:to-purple-600 text-white"
          >
            <Sparkles className="w-4 h-4 mr-2" />
            Begin Optimization
          </Button>
        </div>
      </div>

      {/* Summary Stats */}
      <div className="space-y-2">
        {/* Final label counts */}
        <div className="flex gap-4">
          <div className="flex items-center gap-2 text-sm">
            <div className="w-3 h-3 rounded-full bg-green-500" />
            <span className="text-secondary">{correctCount} correct</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <div className="w-3 h-3 rounded-full bg-red-500" />
            <span className="text-secondary">{incorrectCount} incorrect</span>
          </div>
        </div>

        {/* Auto-label breakdown */}
        {hasAutoLabels && (
          <div className="flex flex-wrap gap-4 text-sm text-muted">
            <div className="flex items-center gap-1.5">
              <Bot className="w-3.5 h-3.5" />
              <span>{autoLabeledCorrect} auto-correct</span>
            </div>
            <div className="flex items-center gap-1.5">
              <Bot className="w-3.5 h-3.5" />
              <span>{autoLabeledIncorrect} auto-incorrect</span>
            </div>
            {inconclusiveCount > 0 && (
              <div className="flex items-center gap-1.5">
                <span className="text-amber-500">{inconclusiveCount} inconclusive</span>
              </div>
            )}
            {overrideCount > 0 && (
              <div className="flex items-center gap-1.5">
                <User className="w-3.5 h-3.5 text-blue-500" />
                <span className="text-blue-600 dark:text-blue-400">{overrideCount} user overrides</span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Questions List */}
      <div className="space-y-4">
        {questions.map((question, index) => {
          const isCorrect = correctAnswers[question.id]
          const feedback = feedbackTexts[question.id]
          const autoResult = autoLabelResults[question.id]
          const isOverridden = userOverrides[question.id] === true
          const overrideReason = overrideReasons[question.id]
          const questionText = question.question.join(" ")

          return (
            <Card key={question.id}>
              <CardContent className="py-4">
                <div className="space-y-2">
                  <div className="flex items-start justify-between gap-4">
                    <p className="text-primary">
                      <span className="text-muted font-mono text-sm mr-2">
                        {index + 1}.
                      </span>
                      {questionText}
                    </p>
                  </div>
                  <div className="flex items-center gap-3 flex-wrap">
                    {/* Auto-label badge */}
                    {autoResult && (
                      <div className={`inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full ${
                        autoResult.auto_label === "correct"
                          ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
                          : autoResult.auto_label === "incorrect"
                          ? "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400"
                          : "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400"
                      }`}>
                        <Bot className="w-3 h-3" />
                        <span className="capitalize">{autoResult.auto_label}</span>
                      </div>
                    )}

                    {/* Override indicator */}
                    {isOverridden && (
                      <div className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400">
                        <User className="w-3 h-3" />
                        <span>Overridden</span>
                      </div>
                    )}

                    {/* Final label */}
                    {isCorrect === true ? (
                      <div className="flex items-center gap-1.5 text-green-600 dark:text-green-400">
                        <Check className="w-4 h-4" />
                        <span className="text-sm font-medium">Correct</span>
                      </div>
                    ) : isCorrect === false ? (
                      <div className="flex items-center gap-1.5 text-red-600 dark:text-red-400">
                        <X className="w-4 h-4" />
                        <span className="text-sm font-medium">Incorrect</span>
                      </div>
                    ) : (
                      <span className="text-sm text-muted">Not labeled</span>
                    )}
                  </div>

                  {/* Auto-label reason */}
                  {autoResult && (
                    <p className="text-xs text-muted italic pl-4">
                      {autoResult.reason}
                    </p>
                  )}

                  {/* Feedback text */}
                  {isCorrect === false && feedback && (
                    <div className="mt-2 pl-4 border-l-2 border-red-300 dark:border-red-700">
                      <p className="text-sm text-secondary">
                        <span className="text-muted">Feedback:</span> {feedback}
                      </p>
                    </div>
                  )}

                  {/* Override reason */}
                  {isOverridden && overrideReason && (
                    <div className="mt-2 pl-4 border-l-2 border-blue-300 dark:border-blue-700">
                      <p className="text-sm text-secondary">
                        <span className="text-muted">Override reason:</span> {overrideReason}
                      </p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}
