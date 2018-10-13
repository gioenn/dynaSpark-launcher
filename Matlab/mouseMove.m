function [x, y] = mouseMove (object, eventdata)

C = get (gca, 'CurrentPoint');
%ch=evalin('base', 'ch');
%label=evalin('base', 'cur_label');
%msg=strcat(ch, ' ', label, ' (X,Y)=(', num2str(C(1,1)), ', ',num2str(C(1,2)), ')');
%title(gca, ['(X,Y)=(', num2str(C(1,1)), ', ',num2str(C(1,2)), ')']);
%title(gca, msg);
x=C(1,1);
y=C(1,2);

% Davide Bertolotti
% davide.bertolotti@mail.polimi.it
% Copyright 2017