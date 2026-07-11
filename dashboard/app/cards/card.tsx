import React from 'react';
import { X, Minimize2, Maximize2 } from 'lucide-react';

interface CardProps {
  id: string;
  title?: string;
  children: React.ReactNode;
  onRemove?: (id: string) => void;
  onToggleMinimize?: (id: string) => void;
  isMinimized?: boolean;
}

const Card: React.FC<CardProps> = ({
  id,
  title,
  children,
  onRemove,
  onToggleMinimize,
  isMinimized = false,
}) => {
  return (
    <div className="bg-black-900/95 backdrop-blur-sm border border-gray-700 shadow-xl flex flex-col h-full overflow-hidden text-gray-200">
      {title && (
        <div className="drag-handle flex items-center justify-between px-3 py-2 bg-gray-800/50 border-b border-gray-700 cursor-move">
          <span className="font-medium text-sm truncate">{title}</span>
          <div className="flex items-center space-x-1">
            {onToggleMinimize && (
              <button
                onClick={() => onToggleMinimize(id)}
                className="p-1 hover:bg-gray-700 rounded transition-colors"
                aria-label="Toggle minimize"
              >
                {isMinimized ? <Maximize2 size={14} /> : <Minimize2 size={14} />}
              </button>
            )}
            {onRemove && (
              <button
                onClick={() => onRemove(id)}
                className="p-1 hover:bg-red-500/20 hover:text-red-400 rounded transition-colors"
                aria-label="Close card"
              >
                <X size={14} />
              </button>
            )}
          </div>
        </div>
      )}
      <div className="flex-1 p-2 overflow-auto">
        {isMinimized ? (
          <div className="flex items-center justify-center h-12 text-gray-500 text-sm">
            — minimized —
          </div>
        ) : (
          children
        )}
      </div>
    </div>
  );
};

export default Card;